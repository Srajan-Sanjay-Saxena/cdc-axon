package walhandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type WalHandlers struct {
	ClientLSN  pglogrepl.LSN
	relations  map[uint32]*pglogrepl.RelationMessage
	Persistor  engine_source.PersistenceStore
	PendingLSN pglogrepl.LSN
}

type HandleResult struct {
	Reply      bool
	Event      event.Event
}

func NewWalHandlers() *WalHandlers {
	return &WalHandlers{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}


// HandleMessage processes a raw message from the Postgres replication stream.
// Replication data = WAL changes (INSERT/UPDATE/DELETE) + server keepalives.
// All of it arrives wrapped in CopyData envelopes — this extracts the payload
// and routes it by type. Non-CopyData messages are ignored.

func (wh *WalHandlers) HandleMessage(ctx context.Context, msg pgproto3.BackendMessage) (HandleResult, error) {
	if m, ok := msg.(*pgproto3.CopyData); ok {
		return wh.handleCopyData(ctx, m.Data)
	}
	return HandleResult{}, nil
}

// handleCopyData demultiplexes the raw CopyData payload by its first byte.
//
// Inside a CopyData message, the first byte is a sub-type identifier:
//   - 'k' (PrimaryKeepaliveMessageByteID): Postgres heartbeat. Sent every
//     wal_sender_timeout/2 (~30s default). Contains server's current WAL
//     position and a ReplyRequested flag. If we don't reply in time,
//     Postgres kills the replication connection.
//   - 'w' (XLogDataByteID): Actual WAL data. Contains the LSN and the
//     logical replication message (RelationMessage, InsertMessage, etc.)
//
// The remaining bytes (data[1:]) are the message payload without the type byte.
func (wh *WalHandlers) handleCopyData(ctx context.Context, data []byte) (HandleResult, error) {
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return wh.handleKeepalive(data[1:])
	case pglogrepl.XLogDataByteID:
		return wh.handleXLogData(ctx, data[1:])
	}
	return HandleResult{}, nil
}

func (wh *WalHandlers) handleKeepalive(data []byte) (HandleResult, error) {
	msg, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return HandleResult{}, err
	}
	if msg.ServerWALEnd > wh.ClientLSN {
		wh.ClientLSN = msg.ServerWALEnd
	}
	return HandleResult{Reply: msg.ReplyRequested}, nil
}

func (wh *WalHandlers) handleXLogData(ctx context.Context, data []byte) (HandleResult, error) {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return HandleResult{}, err
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return HandleResult{}, err
	}

	result := HandleResult{Reply: true}
	lsn := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	switch m := logicalMsg.(type) {
		
	// RelationMessage is table metadata (column names, types, relation ID).
	// Postgres sends it ONCE per table — before the first DML event for that table
	// in the current replication session. Without it, we can't decode tuple data
	// (we wouldn't know which column is "id", "event_type", or "payload").
	// We cache it in memory and persist to store for crash recovery.

	case *pglogrepl.RelationMessage:
		wh.relations[m.RelationID] = m
		data, err := json.Marshal(m)
		if err != nil {
			return HandleResult{}, err
		}
		if wh.Persistor != nil {
			wh.Persistor.Save(ctx, fmt.Sprintf("relation:%d", m.RelationID), data)
		}
		wh.ClientLSN = lsn
	case *pglogrepl.InsertMessage:
		ev, err := wh.buildEventFromTuple(m.RelationID, m.Tuple, event.INSERT)
		if err != nil {
			return HandleResult{}, err
		}
		result.Event = ev
		wh.PendingLSN = lsn
	case *pglogrepl.UpdateMessage:
		ev, err := wh.buildEventFromTuple(m.RelationID, m.NewTuple, event.UPDATE)
		if err != nil {
			return HandleResult{}, err
		}
		result.Event = ev
		wh.PendingLSN = lsn
	case *pglogrepl.DeleteMessage:
		ev, err := wh.buildEventFromTuple(m.RelationID, m.OldTuple, event.DELETE)
		if err != nil {
			return HandleResult{}, err
		}
		result.Event = ev
		wh.PendingLSN = lsn
	case *pglogrepl.BeginMessage, *pglogrepl.CommitMessage:
		wh.ClientLSN = lsn
	}

	return result, nil
}

func (wh *WalHandlers) buildEventFromTuple(relationID uint32, tuple *pglogrepl.TupleData, op event.OperationType) (event.Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rel, ok := wh.relations[relationID]
	if !ok {
		if wh.Persistor == nil {
			return event.Event{}, errors.New("relation metadata missing")
		}
		data, err := wh.Persistor.Load(ctx, fmt.Sprintf("relation:%d", relationID))
		if err != nil || len(data) == 0 {
			return event.Event{}, errors.New("relation metadata missing")
		}
		var loaded pglogrepl.RelationMessage
		if err := json.Unmarshal(data, &loaded); err != nil {
			return event.Event{}, err
		}
		wh.relations[relationID] = &loaded
		rel = &loaded
	}

	if tuple == nil {
		return event.Event{}, errors.New("tuple data is nil")
	}

	var id, eventType string
	var payload []byte

	for i, col := range tuple.Columns {
		if col.DataType == 'n' || col.DataType == 'u' {
			continue
		}
		name := rel.Columns[i].Name
		switch name {
		case "id":
			id = string(col.Data)
		case "event_type":
			eventType = string(col.Data)
		case "payload":
			payload = col.Data
		}
	}

	return event.Event{
		ID:        id,
		Source:    rel.RelationName,
		Operation: op,
		EventType: eventType,
		Payload:   payload,
	}, nil
}

func (wh *WalHandlers) SendStatus(ctx context.Context, conn *pgconn.PgConn) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: wh.ClientLSN,
			WALFlushPosition: wh.ClientLSN,
			WALApplyPosition: wh.ClientLSN,
		},
	)
}

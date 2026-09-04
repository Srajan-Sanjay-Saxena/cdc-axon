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
	
	// The current LSN (Log Sequence Number) that the client has processed and acknowledged to the server.
	ClientLSN  pglogrepl.LSN

	relations  map[uint32]*pglogrepl.RelationMessage
	Persistor  engine_source.PersistenceStore

	// The LSN of the last WAL message that has been processed but not yet acknowledged to the server.
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

// Heartbeat sent by the pg , when there is no ack for a very long time . So that the client can send the ack to the server and server can move forward with its lsn and can delete the wal logs which are already acked by the client.
func (wh *WalHandlers) handleKeepalive(data []byte) (HandleResult, error) {
	msg, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return HandleResult{}, err
	}
	if msg.ServerWALEnd > wh.ClientLSN {
		wh.ClientLSN = msg.ServerWALEnd
	}

	/*
	When Postgres sends ReplyRequested=false, it's just saying:

	"Here's my current WAL position for your information. I don't urgently need a reply right now."
	Postgres sends keepalives periodically (every wal_sender_timeout / 2). Most of them have ReplyRequested=false — it's just a heartbeat to share the server's LSN position.

	ReplyRequested=true happens when:
	Postgres is approaching wal_sender_timeout and hasn't heard from you
	Postgres wants to confirm you're still alive before killing the connection
	*/
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
	
	/*
		DML = Data Manipulation Language
		case *pglogrepl.InsertMessage:  // DML: INSERT
		case *pglogrepl.UpdateMessage:  // DML: UPDATE
		case *pglogrepl.DeleteMessage:  // DML: DELETE
	*/


	// in the current replication session. Without it, we can't decode tuple data
	// (we wouldn't know which column is "id", "event_type", or "payload").
	// We cache it in memory and persist to store for crash recovery.

	/*
		Relation messages are sent when a relation is first used in the stream, or when the relation's schema changes.
		So we don't have to think about the fact that what if we have to make some changes in outbox table.
	*/

		/*

			Table = collection of tuples
			Tuple = one row = [column1, column2, column3, ...]

			What Postgres sends for each operation

			INSERT — sends the new row:
			case *pglogrepl.InsertMessage:
				m.Tuple  // ← the row that was inserted

			UPDATE — sends the new row (after update):
			case *pglogrepl.UpdateMessage:
				m.NewTuple  // ← the row AFTER update
				m.OldTuple  // ← the row BEFORE update (only if REPLICA IDENTITY FULL)

			DELETE — sends the old row (what was deleted):
			case *pglogrepl.DeleteMessage:
				m.OldTuple  // ← the row that was deleted

		*/

	case *pglogrepl.RelationMessage:
		wh.relations[m.RelationID] = m
		data, err := json.Marshal(m)
		if err != nil {
			return HandleResult{}, err
		}
		if wh.Persistor != nil {
			if err := wh.Persistor.Save(ctx, fmt.Sprintf("relation:%d", m.RelationID), data); err != nil {
				return HandleResult{}, err
			}
		}
		wh.ClientLSN = lsn

	// For each of the DML messages , we are only advancing the pending lsn , client will be advanced when only the ack is sent by the sinks that they have recieved the event .

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

	// tuple.Columns and rel.Columns are parallel arrays — same order, same length.
	// tuple.Columns contains the actual row data (just values, no names).
	// rel.Columns contains the schema from RelationMessage (column names and types).
	//
	// Example for INSERT INTO outbox VALUES ('evt-1', 'ORDER_CREATED', '{"x":1}'):
	//
	//   tuple.Columns:  [Data="evt-1"]    [Data="ORDER_CREATED"]   [Data='{"x":1}']
	//                        ↓                    ↓                       ↓
	//                     index 0              index 1                 index 2
	//                        ↓                    ↓                       ↓
	//   rel.Columns:    [Name="id"]       [Name="event_type"]      [Name="payload"]
	//
	// We use the same index i to get the column name from rel.Columns[i].Name,
	// then match it to extract id, event_type, and payload into variables.
	// Each column name is unique in SQL, so each case matches exactly once.

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

		/*
			In Postgres terminology:

			Relation = table
			RelationName = table name
		*/
		Source:    rel.RelationName,

		Operation: op,
		EventType: eventType,
		Payload:   payload,
	}, nil
}

// SendStatus sends an acknowledgment to Postgres with the current LSN position.
// This tells Postgres: "I've processed everything up to ClientLSN, you can clean up WAL before this point."
//
// The three positions (WALWritePosition, WALFlushPosition, WALApplyPosition) exist because
// in a real Postgres standby, receiving, flushing to disk, and applying are separate steps.
// For CDC-Axon, they're all the same — once the event is published to the broker, all three are done.
//
// When this is called:
//   - After successful publish: Ack() sets ClientLSN = PendingLSN, then calls SendStatus
//   - On timeout (no messages for 10s): sends current ClientLSN to keep connection alive
//   - On keepalive with ReplyRequested=true: immediate response to avoid disconnect
//
// If this fails, the connection is likely dead — CaptureEvents will detect it on next
// ReceiveMessage and trigger the engine's retry mechanism.
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

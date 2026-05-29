package walhandler

import (
	"context"
	"errors"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/engine_source"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"encoding/json"
	"fmt"
	"time"
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


func (wh *WalHandlers) HandleMessage(ctx context.Context, msg pgproto3.BackendMessage) (HandleResult, error) {
	if m, ok := msg.(*pgproto3.CopyData); ok {
		return wh.handleCopyData(ctx, m.Data)
	}
	return HandleResult{}, nil
}

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
		event, err := wh.buildEvent(m)
		if err != nil {
			return HandleResult{}, err
		}
		result.Event = event
		wh.PendingLSN = lsn
	case *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage,
		*pglogrepl.BeginMessage, *pglogrepl.CommitMessage:
		wh.ClientLSN = lsn
	}

	return result, nil
}

func (wh *WalHandlers) buildEvent(msg *pglogrepl.InsertMessage) (event.Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rel, ok := wh.relations[msg.RelationID]
	if !ok {
		if wh.Persistor == nil {
			return event.Event{}, errors.New("relation metadata missing")
		}
		data, err := wh.Persistor.Load(ctx, fmt.Sprintf("relation:%d", msg.RelationID))
		if err != nil || len(data) == 0 {
			return event.Event{}, errors.New("relation metadata missing")
		}
		var loaded pglogrepl.RelationMessage
		if err := json.Unmarshal(data, &loaded); err != nil {
			return event.Event{}, err
		}
		wh.relations[msg.RelationID] = &loaded
		rel = &loaded
	}

	var id, eventType string
	var operation event.OperationType
	var payload []byte

	for i, col := range msg.Tuple.Columns {
		name := rel.Columns[i].Name
		switch name {
		case "id":
			id = string(col.Data)
		case "event_type":
			eventType = string(col.Data)
		case "payload":
			payload = col.Data
		case "operation":
			operation = event.OperationType(col.Data)

		}
	}

	return event.Event{
		ID:        id,
		Source:    rel.RelationName,
		Operation: operation,
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

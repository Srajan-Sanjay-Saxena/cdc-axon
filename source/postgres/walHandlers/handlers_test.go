package walhandler

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestHandleMessage_NonCopyData(t *testing.T) {
	wh := NewWalHandlers()

	msg := &pgproto3.NoticeResponse{}
	result, err := wh.HandleMessage(context.Background(), msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Reply {
		t.Error("expected Reply=false for non-CopyData")
	}
	if result.Event.ID != "" {
		t.Error("expected empty Event for non-CopyData")
	}
}

func TestHandleKeepalive_AdvancesLSN(t *testing.T) {
	wh := NewWalHandlers()
	wh.ClientLSN = 100

	data := make([]byte, 17)
	binary.BigEndian.PutUint64(data[0:8], 200)
	binary.BigEndian.PutUint64(data[8:16], 0)
	data[16] = 1

	result, err := wh.handleKeepalive(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if wh.ClientLSN != 200 {
		t.Errorf("expected ClientLSN=200, got %v", wh.ClientLSN)
	}
	if !result.Reply {
		t.Error("expected Reply=true when ReplyRequested=1")
	}
}

func TestHandleKeepalive_DoesNotRegressLSN(t *testing.T) {
	wh := NewWalHandlers()
	wh.ClientLSN = 500

	data := make([]byte, 17)
	binary.BigEndian.PutUint64(data[0:8], 300)
	binary.BigEndian.PutUint64(data[8:16], 0)
	data[16] = 0

	_, err := wh.handleKeepalive(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if wh.ClientLSN != 500 {
		t.Errorf("expected ClientLSN to stay at 500, got %v", wh.ClientLSN)
	}
}

func TestHandleKeepalive_NoReplyWhenNotRequested(t *testing.T) {
	wh := NewWalHandlers()

	data := make([]byte, 17)
	binary.BigEndian.PutUint64(data[0:8], 100)
	binary.BigEndian.PutUint64(data[8:16], 0)
	data[16] = 0

	result, err := wh.handleKeepalive(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Reply {
		t.Error("expected Reply=false when ReplyRequested=0")
	}
}

func TestBuildEvent_MissingRelation(t *testing.T) {
	wh := NewWalHandlers()

	msg := &pglogrepl.InsertMessage{
		RelationID: 999,
	}

	_, err := wh.buildEvent(msg)
	if err == nil {
		t.Error("expected error for missing relation")
	}
	if err.Error() != "relation metadata missing" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestBuildEvent_Success(t *testing.T) {
	wh := NewWalHandlers()

	wh.relations[1] = &pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: "outbox",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"},
			{Name: "event_type"},
			{Name: "operation"},
			{Name: "payload"},
		},
	}

	msg := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("test-1")},
				{Data: []byte("ORDER_CREATED")},
				{Data: []byte("insert")},
				{Data: []byte(`{"orderId": 123}`)},
			},
		},
	}

	ev, err := wh.buildEvent(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ev.Source != "outbox" {
		t.Errorf("expected Source=outbox, got %s", ev.Source)
	}
	if ev.EventType != "ORDER_CREATED" {
		t.Errorf("expected EventType=ORDER_CREATED, got %s", ev.EventType)
	}
	if string(ev.Payload) != `{"orderId": 123}` {
		t.Errorf("unexpected payload: %s", string(ev.Payload))
	}
}

func TestBuildEvent_NoPayloadColumn(t *testing.T) {
	wh := NewWalHandlers()

	wh.relations[1] = &pglogrepl.RelationMessage{
		RelationID:   1,
		RelationName: "outbox",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"},
			{Name: "event_type"},
		},
	}

	msg := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: []byte("test-1")},
				{Data: []byte("ORDER_CREATED")},
			},
		},
	}

	ev, err := wh.buildEvent(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ev.Payload != nil {
		t.Errorf("expected nil payload when no payload column, got %s", string(ev.Payload))
	}
}

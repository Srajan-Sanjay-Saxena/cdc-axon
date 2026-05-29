package source

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
	"github.com/jackc/pglogrepl"
)

func TestNewSource(t *testing.T) {
	cfg := &config.PgRelaySourceConfig{
		URL:             "postgres://user:pass@localhost:5432/dbname",
		SlotName:        "myslot",
		PublicationName: "mypub",
	}
	s := NewSource(cfg)

	if s.walHandler == nil {
		t.Error("expected walHandler to be initialized")
	}
	if s.cfg != cfg {
		t.Error("expected cfg to be set")
	}
}

func TestAddProducer(t *testing.T) {
	cfg := &config.PgRelaySourceConfig{}
	s := NewSource(cfg)
	mock := &MockProducer{}

	s.AddProducer(mock)

	prod, err := s.GetProducer()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prod != mock {
		t.Error("expected producer to be set")
	}
}

func TestGetProducer_NotInitialized(t *testing.T) {
	cfg := &config.PgRelaySourceConfig{}
	s := NewSource(cfg)

	_, err := s.GetProducer()
	if err == nil {
		t.Error("expected error when producer not initialized")
	}
}

func TestLSNNotAdvancedOnPublishFailure(t *testing.T) {
	mock := &MockProducer{shouldFail: true}
	wh := newTestWalHandler()
	wh.ClientLSN = 100

	e := event.Event{
		ID:        "test-1",
		Source:    "outbox",
		Operation: event.INSERT,
		EventType: "ORDER_CREATED",
		Payload:   json.RawMessage(`{"id": 1}`),
	}

	err := mock.Publish(context.Background(), e)
	if err == nil {
		t.Fatal("expected publish to fail")
	}

	if wh.ClientLSN != 100 {
		t.Errorf("expected ClientLSN to stay at 100, got %v", wh.ClientLSN)
	}
}

func TestLSNAdvancesOnPublishSuccess(t *testing.T) {
	mock := &MockProducer{}
	wh := newTestWalHandler()
	wh.ClientLSN = 100

	e := event.Event{
		ID:        "test-1",
		Source:    "outbox",
		Operation: event.INSERT,
		EventType: "ORDER_CREATED",
		Payload:   json.RawMessage(`{"orderId": 123}`),
	}

	err := mock.Publish(context.Background(), e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wh.ClientLSN = pglogrepl.LSN(300)

	if wh.ClientLSN != 300 {
		t.Errorf("expected ClientLSN=300, got %v", wh.ClientLSN)
	}
	if len(mock.events) != 1 {
		t.Errorf("expected 1 published event, got %d", len(mock.events))
	}
	if string(mock.events[0].Payload) != `{"orderId": 123}` {
		t.Errorf("unexpected payload: %s", string(mock.events[0].Payload))
	}
}

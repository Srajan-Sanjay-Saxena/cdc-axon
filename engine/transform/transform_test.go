package transform

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
)

// mockDeduplicationStore — in-memory DeduplicationStore for testing
type mockDeduplicationStore struct {
	data map[string][]byte
}

func newMockDeduplicationStore() *mockDeduplicationStore {
	return &mockDeduplicationStore{data: make(map[string][]byte)}
}

func (m *mockDeduplicationStore) SaveWithTTL(_ context.Context, key string, value []byte, _ time.Duration) error {
	m.data[key] = value
	return nil
}

func (m *mockDeduplicationStore) Exists(_ context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

// --- Deduplicate Tests ---

func TestDeduplicate_FirstEventPasses(t *testing.T) {
	store := newMockDeduplicationStore()
	dedup := Deduplicate(store, 1*time.Hour)
	ctx := context.Background()

	e := event.Event{ID: "evt-1", EventType: "ORDER_CREATED"}

	result, keep, err := dedup(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Error("expected first event to pass through")
	}
	if result.ID != "evt-1" {
		t.Errorf("expected ID=evt-1, got %s", result.ID)
	}
}

func TestDeduplicate_DuplicateDropped(t *testing.T) {
	store := newMockDeduplicationStore()
	dedup := Deduplicate(store, 1*time.Hour)
	ctx := context.Background()

	e := event.Event{ID: "evt-1", EventType: "ORDER_CREATED"}

	// first call — passes
	_, keep, _ := dedup(ctx, e)
	if !keep {
		t.Fatal("first event should pass")
	}

	// second call — same ID, should be dropped
	_, keep, err := dedup(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keep {
		t.Error("expected duplicate to be dropped")
	}
}

func TestDeduplicate_DifferentIDsBothPass(t *testing.T) {
	store := newMockDeduplicationStore()
	dedup := Deduplicate(store, 1*time.Hour)
	ctx := context.Background()

	e1 := event.Event{ID: "evt-1", EventType: "ORDER_CREATED"}
	e2 := event.Event{ID: "evt-2", EventType: "ORDER_SHIPPED"}

	_, keep, _ := dedup(ctx, e1)
	if !keep {
		t.Error("evt-1 should pass")
	}

	_, keep, _ = dedup(ctx, e2)
	if !keep {
		t.Error("evt-2 should pass (different ID)")
	}
}

func TestDeduplicate_NilStorePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic with nil store")
		}
	}()
	Deduplicate(nil, 1*time.Hour)
}

// --- FilterByEventType Tests ---

func TestFilterByEventType_Allowed(t *testing.T) {
	f := FilterByEventType("ORDER_CREATED", "ORDER_SHIPPED")
	ctx := context.Background()

	e := event.Event{EventType: "ORDER_CREATED"}
	_, keep, err := f(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Error("expected ORDER_CREATED to pass")
	}
}

func TestFilterByEventType_Blocked(t *testing.T) {
	f := FilterByEventType("ORDER_CREATED", "ORDER_SHIPPED")
	ctx := context.Background()

	e := event.Event{EventType: "HEARTBEAT"}
	_, keep, err := f(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keep {
		t.Error("expected HEARTBEAT to be dropped")
	}
}

// --- FilterByOperation Tests ---

func TestFilterByOperation_Allowed(t *testing.T) {
	f := FilterByOperation(event.INSERT, event.UPDATE)
	ctx := context.Background()

	e := event.Event{Operation: event.INSERT}
	_, keep, _ := f(ctx, e)
	if !keep {
		t.Error("expected INSERT to pass")
	}
}

func TestFilterByOperation_Blocked(t *testing.T) {
	f := FilterByOperation(event.INSERT)
	ctx := context.Background()

	e := event.Event{Operation: event.DELETE}
	_, keep, _ := f(ctx, e)
	if keep {
		t.Error("expected DELETE to be dropped")
	}
}

// --- AddHeader Tests ---

func TestAddHeader(t *testing.T) {
	f := AddHeader("service", []byte("order-svc"))
	ctx := context.Background()

	e := event.Event{ID: "evt-1"}
	result, keep, _ := f(ctx, e)
	if !keep {
		t.Error("AddHeader should always keep")
	}
	if string(result.Headers["service"]) != "order-svc" {
		t.Errorf("expected header service=order-svc, got %s", string(result.Headers["service"]))
	}
}

// --- MaskField Tests ---

func TestMaskField(t *testing.T) {
	f := MaskField("email", "phone")
	ctx := context.Background()

	payload := `{"orderId": 1, "email": "john@example.com", "phone": "555-1234", "amount": 99.99}`
	e := event.Event{Payload: json.RawMessage(payload)}

	result, keep, err := f(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Error("MaskField should always keep")
	}

	var m map[string]interface{}
	json.Unmarshal(result.Payload, &m)

	if m["email"] != "***" {
		t.Errorf("expected email=***, got %v", m["email"])
	}
	if m["phone"] != "***" {
		t.Errorf("expected phone=***, got %v", m["phone"])
	}
	if m["amount"] != 99.99 {
		t.Errorf("expected amount=99.99, got %v", m["amount"])
	}
}

func TestMaskField_NilPayload(t *testing.T) {
	f := MaskField("email")
	ctx := context.Background()

	e := event.Event{Payload: nil}
	_, keep, err := f(ctx, e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Error("should pass through nil payload")
	}
}

// --- AddTimestamp Tests ---

func TestAddTimestamp(t *testing.T) {
	f := AddTimestamp()
	ctx := context.Background()

	before := time.Now().UnixNano()
	e := event.Event{ID: "evt-1"}
	result, keep, _ := f(ctx, e)
	after := time.Now().UnixNano()

	if !keep {
		t.Error("AddTimestamp should always keep")
	}

	raw := result.Headers["captured_at"]
	if raw == nil {
		t.Fatal("expected captured_at header")
	}

	ts := int64(binary.BigEndian.Uint64(raw))
	if ts < before || ts > after {
		t.Errorf("timestamp %d not in range [%d, %d]", ts, before, after)
	}
}

// --- RouteByEventType Tests ---

func TestRouteByEventType_Match(t *testing.T) {
	f := RouteByEventType(map[string]string{
		"ORDER_CREATED": "orders.queue",
		"default":       "misc.queue",
	})
	ctx := context.Background()

	e := event.Event{EventType: "ORDER_CREATED"}
	result, keep, _ := f(ctx, e)
	if !keep {
		t.Error("RouteByEventType should always keep")
	}
	if string(result.Headers["routing_key"]) != "orders.queue" {
		t.Errorf("expected routing_key=orders.queue, got %s", string(result.Headers["routing_key"]))
	}
}

func TestRouteByEventType_Default(t *testing.T) {
	f := RouteByEventType(map[string]string{
		"ORDER_CREATED": "orders.queue",
		"default":       "misc.queue",
	})
	ctx := context.Background()

	e := event.Event{EventType: "SOMETHING_ELSE"}
	result, _, _ := f(ctx, e)
	if string(result.Headers["routing_key"]) != "misc.queue" {
		t.Errorf("expected routing_key=misc.queue, got %s", string(result.Headers["routing_key"]))
	}
}

// --- SampleRate Tests ---

func TestSampleRate(t *testing.T) {
	f := SampleRate(3)
	ctx := context.Background()
	e := event.Event{ID: "evt"}

	var kept int
	for i := 0; i < 9; i++ {
		_, keep, _ := f(ctx, e)
		if keep {
			kept++
		}
	}

	if kept != 3 {
		t.Errorf("expected 3 events kept out of 9, got %d", kept)
	}
}

package metrics_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/metrics"
)

// recordingMetrics records every metrics call for assertion.
type recordingMetrics struct {
	mu               sync.Mutex
	captured         []capturedCall
	dropped          []droppedCall
	published        []publishedCall
	publishFailed    []string
	acks             []string
	retries          int
	snapshotProgress []snapshotCall
}

type capturedCall struct {
	source, operation string
}
type droppedCall struct {
	source, reason string
}
type publishedCall struct {
	producer string
	duration time.Duration
}
type snapshotCall struct {
	table string
	rows  int64
}

func (r *recordingMetrics) EventCaptured(_ context.Context, source, operation string) {
	r.mu.Lock()
	r.captured = append(r.captured, capturedCall{source, operation})
	r.mu.Unlock()
}
func (r *recordingMetrics) EventDropped(_ context.Context, source, reason string) {
	r.mu.Lock()
	r.dropped = append(r.dropped, droppedCall{source, reason})
	r.mu.Unlock()
}
func (r *recordingMetrics) EventPublished(_ context.Context, producer string, duration time.Duration) {
	r.mu.Lock()
	r.published = append(r.published, publishedCall{producer, duration})
	r.mu.Unlock()
}
func (r *recordingMetrics) PublishFailed(_ context.Context, producer string) {
	r.mu.Lock()
	r.publishFailed = append(r.publishFailed, producer)
	r.mu.Unlock()
}
func (r *recordingMetrics) AckCompleted(_ context.Context, source string) {
	r.mu.Lock()
	r.acks = append(r.acks, source)
	r.mu.Unlock()
}
func (r *recordingMetrics) RetryTriggered(_ context.Context) {
	r.mu.Lock()
	r.retries++
	r.mu.Unlock()
}
func (r *recordingMetrics) SnapshotProgress(_ context.Context, table string, rows int64) {
	r.mu.Lock()
	r.snapshotProgress = append(r.snapshotProgress, snapshotCall{table, rows})
	r.mu.Unlock()
}

var _ metrics.Metrics = (*recordingMetrics)(nil)

// mockSource is a minimal EngineSource that emits N events then blocks until ctx is done.
type mockSource struct {
	events    []event.Event
	producers []engine_source.Producer
	mu        sync.Mutex
	ackCount  int
}

func (s *mockSource) DBConnect(_ context.Context) error { return nil }
func (s *mockSource) Close(_ context.Context) error     { return nil }
func (s *mockSource) Ack(_ context.Context) error {
	s.mu.Lock()
	s.ackCount++
	s.mu.Unlock()
	return nil
}
func (s *mockSource) GetProducers() ([]engine_source.Producer, error) {
	return s.producers, nil
}
func (s *mockSource) CaptureEvents(ctx context.Context) (<-chan event.Event, error) {
	ch := make(chan event.Event)
	go func() {
		defer close(ch)
		for _, ev := range s.events {
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
		// block until context is cancelled — simulates a live stream that goes quiet
		<-ctx.Done()
	}()
	return ch, nil
}

// mockProducer records publishes.
type mockProducer struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *mockProducer) Connect(_ context.Context) error { return nil }
func (p *mockProducer) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
	return nil
}
func (p *mockProducer) Close() error { return nil }

// failingProducer always returns an error on Publish.
type failingProducer struct{}

func (p *failingProducer) Connect(_ context.Context) error              { return nil }
func (p *failingProducer) Publish(_ context.Context, _ event.Event) error { return fmt.Errorf("broker down") }
func (p *failingProducer) Close() error                                 { return nil }

// TestMetrics_EventCapturedAndPublished verifies that EventCaptured and
// EventPublished are called for each successfully processed event.
func TestMetrics_EventCapturedAndPublished(t *testing.T) {
	rec := &recordingMetrics{}
	prod := &mockProducer{}

	src := &mockSource{
		events: []event.Event{
			{ID: "1", Source: "outbox", Operation: event.INSERT, EventType: "ORDER_CREATED"},
			{ID: "2", Source: "outbox", Operation: event.UPDATE, EventType: "ORDER_UPDATED"},
		},
		producers: []engine_source.Producer{prod},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go core.New(src).SetMetrics(rec).Start(ctx)

	// wait for events to be processed
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.captured) != 2 {
		t.Fatalf("expected 2 EventCaptured calls, got %d", len(rec.captured))
	}
	if rec.captured[0].source != "outbox" || rec.captured[0].operation != "insert" {
		t.Errorf("unexpected first capture: %+v", rec.captured[0])
	}
	if rec.captured[1].operation != "update" {
		t.Errorf("expected update operation, got %s", rec.captured[1].operation)
	}
	if len(rec.published) != 2 {
		t.Fatalf("expected 2 EventPublished calls, got %d", len(rec.published))
	}
	for _, p := range rec.published {
		if p.duration <= 0 {
			t.Errorf("expected positive duration, got %v", p.duration)
		}
	}
}

// TestMetrics_EventDropped verifies that EventDropped is called when a
// transform drops an event.
func TestMetrics_EventDropped(t *testing.T) {
	rec := &recordingMetrics{}
	prod := &mockProducer{}

	src := &mockSource{
		events: []event.Event{
			{ID: "1", Source: "outbox", Operation: event.INSERT, EventType: "KEEP"},
			{ID: "2", Source: "outbox", Operation: event.INSERT, EventType: "DROP"},
			{ID: "3", Source: "outbox", Operation: event.INSERT, EventType: "KEEP"},
		},
		producers: []engine_source.Producer{prod},
	}

	dropFilter := func(_ context.Context, e event.Event) (event.Event, bool, error) {
		return e, e.EventType != "DROP", nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go core.New(src).SetMetrics(rec).AddTransform(dropFilter).Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.captured) != 3 {
		t.Fatalf("expected 3 EventCaptured calls, got %d", len(rec.captured))
	}
	if len(rec.dropped) != 1 {
		t.Fatalf("expected 1 EventDropped call, got %d", len(rec.dropped))
	}
	if rec.dropped[0].reason != "transform" {
		t.Errorf("expected reason=transform, got %s", rec.dropped[0].reason)
	}
	if len(rec.published) != 2 {
		t.Fatalf("expected 2 EventPublished calls, got %d", len(rec.published))
	}
}

// TestMetrics_PublishFailed verifies that PublishFailed is called when
// a producer fails.
func TestMetrics_PublishFailed(t *testing.T) {
	rec := &recordingMetrics{}

	src := &mockSource{
		events: []event.Event{
			{ID: "1", Source: "outbox", Operation: event.INSERT, EventType: "TEST"},
		},
		producers: []engine_source.Producer{&failingProducer{}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go core.New(src).SetMetrics(rec).Start(ctx)

	// wait for at least one retry
	time.Sleep(2 * time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.captured) == 0 {
		t.Fatal("expected at least 1 EventCaptured call")
	}
	if len(rec.publishFailed) == 0 {
		t.Fatal("expected at least 1 PublishFailed call")
	}
	if rec.retries == 0 {
		t.Fatal("expected at least 1 RetryTriggered call")
	}
}

// TestMetrics_AckCompleted verifies that AckCompleted is called after
// successful publish + ack.
func TestMetrics_AckCompleted(t *testing.T) {
	rec := &recordingMetrics{}
	prod := &mockProducer{}

	src := &mockSource{
		events: []event.Event{
			{ID: "1", Source: "outbox", Operation: event.INSERT, EventType: "TEST"},
		},
		producers: []engine_source.Producer{prod},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go core.New(src).SetMetrics(rec).Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.acks) != 1 {
		t.Fatalf("expected 1 AckCompleted call, got %d", len(rec.acks))
	}
	if rec.acks[0] != "outbox" {
		t.Errorf("expected source=outbox, got %s", rec.acks[0])
	}
}

// TestMetrics_NilMetrics verifies the engine works fine without metrics set
// (nil metrics, no panic).
func TestMetrics_NilMetrics(t *testing.T) {
	prod := &mockProducer{}

	src := &mockSource{
		events: []event.Event{
			{ID: "1", Source: "outbox", Operation: event.INSERT, EventType: "TEST"},
		},
		producers: []engine_source.Producer{prod},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go core.New(src).Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	prod.mu.Lock()
	defer prod.mu.Unlock()

	if len(prod.events) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(prod.events))
	}
}

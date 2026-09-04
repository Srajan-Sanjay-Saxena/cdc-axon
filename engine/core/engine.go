package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Srajan-Sanjay-Saxena/Exponential_Backoff/backoff"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/logger"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/metrics"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/snapshot"
)

// Engine is the central orchestrator of CDC-Axon. It coordinates:
// - Source connection (Postgres WAL / Mongo change stream)
// - Snapshot execution (optional, reads existing table data before streaming)
// - Transform pipeline (filter, mask, enrich, deduplicate, rate limit)
// - Concurrent publishing to multiple producers
// - Acknowledgement (LSN advance / resume token save)
// - Retry with exponential backoff on any failure
type Engine struct {
	source      engine_source.EngineSource
	backoff     *backoff.Backoff
	log         *logger.Logger
	transforms  []engine_source.Transform
	snapshotEng *snapshot.SnapshotEngine
	metrics     metrics.Metrics
}

// New creates an Engine bound to the given source.
// Default backoff: initial=1s, max=60s, reset after 5min of stability.
func New(src engine_source.EngineSource) *Engine {
	return &Engine{
		source:  src,
		backoff: backoff.New(1*time.Second, 60*time.Second, 5*time.Minute),
		log:     logger.Default(),
	}
}

// SetMetrics overrides the default no-op metrics collector.
func (e *Engine) SetMetrics(m metrics.Metrics) *Engine {
	e.metrics = m
	return e
}

// SetLogger overrides the default logger.
func (e *Engine) SetLogger(l *logger.Logger) *Engine {
	e.log = l
	return e
}

// AddTransform appends a transform to the pipeline.
// Transforms run sequentially in the order they are added.
// Each transform receives the output of the previous one.
func (e *Engine) AddTransform(t engine_source.Transform) *Engine {
	e.transforms = append(e.transforms, t)
	return e
}

// BindSnapshotEngine attaches a snapshot engine that runs before WAL streaming.
// Snapshot reads all existing rows from the table and emits them as events.
// These events flow through the same transform pipeline and producers.
func (e *Engine) BindSnapshotEngine(s *snapshot.SnapshotEngine) *Engine {
	e.snapshotEng = s
	return e
}

// Start is the main entry point. It runs the engine in a retry loop.
// On any error, it backs off and retries. On context cancellation, it exits cleanly.
// This method blocks until context is cancelled or an unrecoverable error occurs.
func (e *Engine) Start(ctx context.Context) error {
	for {
		e.backoff.RecordStart()

		if err := e.run(ctx); err != nil {
			if ctx.Err() != nil {
				e.log.Info("cdc-axon: shutting down")
				return nil
			}
			e.log.Error("cdc-axon: run error", "error", err)
			if e.metrics != nil {
				e.metrics.RetryTriggered(ctx)
			}
			if err := e.backoff.Wait(ctx); err != nil {
				e.log.Info("cdc-axon: shutting down")
				return nil
			}
			continue
		}

		return nil
	}
}

// run executes one full lifecycle: connect → snapshot → stream → process.
// If any step fails, it returns an error and Start() will retry with backoff.
// The lifecycle order is:
//   1. Connect to source (Postgres replication / Mongo client)
//   2. Connect all producers concurrently
//   3. Run snapshot if bound (reads existing data, publishes as events)
//   4. Stream live events from WAL/change stream (infinite loop until error or ctx cancel)
func (e *Engine) run(ctx context.Context) error {
	e.log.Debug("cdc-axon: connecting to source")
	if err := e.source.DBConnect(ctx); err != nil {
		return fmt.Errorf("source connect: %w", err)
	}
	defer e.source.Close(ctx)

	producers, err := e.connectProducers(ctx)
	if err != nil {
		return err
	}
	defer e.closeProducers(producers)

	if e.snapshotEng != nil {
		if err := e.runSnapshot(ctx, producers); err != nil {
			return err
		}
	}

	return e.streamEvents(ctx, producers)
}

// connectProducers connects all producers concurrently using goroutines.
// Each producer connects independently in its own goroutine.
// If any producer fails to connect, returns the first error encountered.
// Respects context cancellation — if ctx is cancelled (shutdown), returns immediately.
// On any failure, all successfully connected producers are closed to prevent leaks.
func (e *Engine) connectProducers(ctx context.Context) ([]engine_source.Producer, error) {
	producers, err := e.source.GetProducers()
	if err != nil {
		return nil, fmt.Errorf("get producers: %w", err)
	}

	e.log.Debug("cdc-axon: connecting producers", "count", len(producers))

	// Buffered channel sized to len(producers) — each goroutine writes exactly once,
	// so writes never block even if we exit early due to context cancellation.
	errCh := make(chan error, len(producers))

	// Spawn one goroutine per producer — all connect attempts run in parallel.
	// The closure captures `prod` by parameter (not loop variable) to avoid race.
	for _, prod := range producers {
		go func(prod engine_source.Producer) {
			errCh <- prod.Connect(ctx)
		}(prod)
	}

	// Collect results from all goroutines. We need to read exactly len(producers)
	// values from errCh, but we also need to respect context cancellation.
	//
	// Why select with ctx.Done()?
	//   If a producer's Connect hangs (unresponsive broker, network timeout),
	//   we'd block forever without this. Context cancellation lets us bail out
	//   on shutdown signals instead of waiting indefinitely.
	//
	// Why track firstErr instead of returning immediately?
	//   If we return on first error, other goroutines are still running.
	//   We wait for all to finish (or ctx cancel) so we know the full state
	//   before cleanup. Also, some producers may have connected successfully —
	//   we need to close them to prevent resource leaks.
	var firstErr error
	for i := 0; i < len(producers); i++ {
		select {
		case err := <-errCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			// Shutdown requested — close any producers that may have connected
			// and return immediately. Goroutines still in-flight will eventually
			// write to errCh (buffered, won't block) and exit.
			e.closeProducers(producers)
			return nil, ctx.Err()
		}
	}

	// If any producer failed, close all (including successful ones) and return error.
	// This prevents leaking connections from producers that did connect.
	if firstErr != nil {
		e.closeProducers(producers)
		return nil, fmt.Errorf("producer connect: %w", firstErr)
	}

	return producers, nil
}

// closeProducers closes all producer connections.
// Called via defer in run() to ensure cleanup on any exit path.
func (e *Engine) closeProducers(producers []engine_source.Producer) {
	for _, p := range producers {
		p.Close()
	}
}

// runSnapshot executes the snapshot engine before WAL streaming begins.
// It creates a channel, runs SyncSnapshot in a goroutine (which pushes events),
// and consumes events on the main goroutine (transforms + publish).
//
// Uses select with ctx.Done() because the snapshot channel is non-deterministic —
// could produce 0 events or 10 million. If the snapshot goroutine hangs
// (unresponsive DB), we can still exit via ctx.Done().
//
// Snapshot events do NOT call Ack (ack=false) because there's no WAL LSN to advance —
// snapshot reads from a cursor, not the replication stream.
func (e *Engine) runSnapshot(ctx context.Context, producers []engine_source.Producer) error {
	snapshotCh := make(chan event.Event)
	snapshotErrCh := make(chan error, 1)

	go func() {
		snapshotErrCh <- e.snapshotEng.SyncSnapshot(ctx, snapshotCh)
		close(snapshotCh)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-snapshotCh:
			if !ok {
				return <-snapshotErrCh
			}
			if err := e.processEvent(ctx, producers, ev, false); err != nil {
				return err
			}
		}
	}
}

// streamEvents starts capturing live events from WAL/change stream and processes them.
// This is the main infinite loop that runs until context is cancelled or an error occurs.
//
// Uses select with ctx.Done() because the event channel is an infinite stream —
// on quiet tables, no events may come for hours. Need to be able to shut down.
//
// WAL events call Ack (ack=true) after successful publish — this advances the LSN
// so Postgres knows we've processed up to that point and won't resend.
func (e *Engine) streamEvents(ctx context.Context, producers []engine_source.Producer) error {
	e.log.Debug("cdc-axon: starting event capture")
	events, err := e.source.CaptureEvents(ctx)
	if err != nil {
		return fmt.Errorf("source capture events: %w", err)
	}

	e.log.Info("cdc-axon: engine running, waiting for events")
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("event channel closed")
			}
			if err := e.processEvent(ctx, producers, ev, true); err != nil {
				return err
			}
		}
	}
}

// processEvent handles a single event through the full pipeline:
//   1. Apply transforms (filter, mask, enrich, etc.)
//   2. If dropped by transform → ack (if needed) and return
//   3. Publish to all producers concurrently
//   4. Ack to source (if needed) — advances LSN/resume token
//
// The ack parameter controls whether to call source.Ack() after processing:
//   - true for WAL/stream events: must advance LSN so source doesn't resend
//   - false for snapshot events: no LSN to advance, cursor handles position internally
func (e *Engine) processEvent(ctx context.Context, producers []engine_source.Producer, ev event.Event, ack bool) error {
	e.log.Debug("cdc-axon: event received", "id", ev.ID, "type", ev.EventType)
	if e.metrics != nil {
		e.metrics.EventCaptured(ctx, ev.Source, string(ev.Operation))
	}

	ev, keep, err := e.applyTransforms(ctx, ev)
	if err != nil {
		return fmt.Errorf("transform failed: %w", err)
	}
	if !keep {
		e.log.Debug("cdc-axon: event dropped by transform", "id", ev.ID)
		if e.metrics != nil {
			e.metrics.EventDropped(ctx, ev.Source, "transform")
		}
		if ack {
			if err := e.source.Ack(ctx); err != nil {
				return fmt.Errorf("ack failed: %w", err)
			}
		}
		return nil
	}

	if err := e.publishAll(ctx, producers, ev); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	if ack {
		if err := e.source.Ack(ctx); err != nil {
			return fmt.Errorf("ack failed: %w", err)
		}
		if e.metrics != nil {
			e.metrics.AckCompleted(ctx, ev.Source)
		}
		e.log.Debug("cdc-axon: event acked", "id", ev.ID)
	}

	return nil
}

// publishAll publishes an event to ALL producers concurrently.
// Each producer publishes in its own goroutine. We wait for all to finish.
// If ANY producer fails, the error is returned — and Ack will NOT be called
// (ensuring at-least-once delivery: source will resend the event on next run).
//
// Uses buffered error channel + WaitGroup. No select needed because
// N is deterministic (len(producers)) and each goroutine writes exactly once.
func (e *Engine) publishAll(ctx context.Context, producers []engine_source.Producer, ev event.Event) error {
	type result struct {
		err      error
		producer int
		duration time.Duration
	}

	resCh := make(chan result, len(producers))

	var wg sync.WaitGroup
	for i, p := range producers {
		wg.Add(1)
		go func(prod engine_source.Producer, idx int) {
			defer wg.Done()
			start := time.Now()
			err := prod.Publish(ctx, ev)
			resCh <- result{err: err, producer: idx, duration: time.Since(start)}
		}(p, i)
	}

	wg.Wait()
	close(resCh)

	var firstErr error
	for r := range resCh {
		prodName := fmt.Sprintf("producer_%d", r.producer)
		if r.err != nil {
			if e.metrics != nil {
				e.metrics.PublishFailed(ctx, prodName)
			}
			if firstErr == nil {
				firstErr = r.err
			}
		} else {
			if e.metrics != nil {
				e.metrics.EventPublished(ctx, prodName, r.duration)
			}
		}
	}
	return firstErr
}

// applyTransforms runs the event through the transform chain sequentially.
// Each transform receives the output of the previous one.
// If any transform returns keep=false, the chain short-circuits (event dropped).
// If any transform returns an error, the chain stops and error propagates up.
func (e *Engine) applyTransforms(ctx context.Context, ev event.Event) (event.Event, bool, error) {
	for _, t := range e.transforms {
		var keep bool
		var err error
		ev, keep, err = t(ctx, ev)
		if err != nil {
			return ev, false, err
		}
		if !keep {
			return ev, false, nil
		}
	}
	return ev, true, nil
}

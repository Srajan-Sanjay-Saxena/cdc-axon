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
)

type Engine struct {
	source     engine_source.EngineSource
	backoff    *backoff.Backoff
	log        *logger.Logger
	transforms []engine_source.Transform
}

func New(src engine_source.EngineSource) *Engine {
	return &Engine{
		source:  src,
		backoff: backoff.New(1*time.Second, 60*time.Second, 5*time.Minute),
		log:     logger.Default(),
	}
}

func (e *Engine) SetLogger(l *logger.Logger) *Engine {
	e.log = l
	return e
}

func (e *Engine) AddTransform(t engine_source.Transform) *Engine {
	e.transforms = append(e.transforms, t)
	return e
}

func (e *Engine) Start(ctx context.Context) error {
	for {
		e.backoff.RecordStart()

		if err := e.run(ctx); err != nil {
			if ctx.Err() != nil {
				e.log.Info("cdc-axon: shutting down")
				return nil
			}
			e.log.Error("cdc-axon: run error", "error", err)
			if err := e.backoff.Wait(ctx); err != nil {
				e.log.Info("cdc-axon: shutting down")
				return nil
			}
			continue
		}

		return nil
	}
}

func (e *Engine) run(ctx context.Context) error {
	e.log.Debug("cdc-axon: connecting to source")
	if err := e.source.DBConnect(ctx); err != nil {
		return fmt.Errorf("source connect: %w", err)
	}
	defer e.source.Close(ctx)

	e.log.Debug("cdc-axon: starting event capture")
	events, err := e.source.CaptureEvents(ctx)
	if err != nil {
		return fmt.Errorf("source capture events: %w", err)
	}

	producers, err := e.source.GetProducers()
	if err != nil {
		return fmt.Errorf("get producers: %w", err)
	}

	e.log.Debug("cdc-axon: connecting producers", "count", len(producers))
	for _, p := range producers {
		if err := p.Connect(ctx); err != nil {
			return fmt.Errorf("producer connect: %w", err)
		}
	}
	defer func() {
		for _, p := range producers {
			p.Close()
		}
	}()

	e.log.Info("cdc-axon: engine running, waiting for events")
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("event channel closed")
			}

			e.log.Debug("cdc-axon: event received", "id", ev.ID, "type", ev.EventType)

			ev, keep, err := e.applyTransforms(ctx, ev)
			if err != nil {
				return fmt.Errorf("transform failed: %w", err)
			}
			if !keep {
				e.log.Debug("cdc-axon: event dropped by transform", "id", ev.ID)
				if err := e.source.Ack(ctx); err != nil {
					return fmt.Errorf("ack failed: %w", err)
				}
				continue
			}

			if err := e.publishAll(ctx, producers, ev); err != nil {
				return fmt.Errorf("publish failed: %w", err)
			}

			if err := e.source.Ack(ctx); err != nil {
				return fmt.Errorf("ack failed: %w", err)
			}

			e.log.Debug("cdc-axon: event acked", "id", ev.ID)
		}
	}
}

func (e *Engine) publishAll(ctx context.Context, producers []engine_source.Producer, ev event.Event) error {
	errCh := make(chan error, len(producers))

	var wg sync.WaitGroup
	for _, p := range producers {
		wg.Add(1)
		go func(prod engine_source.Producer) {
			defer wg.Done()
			errCh <- prod.Publish(ctx, ev)
		}(p)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

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

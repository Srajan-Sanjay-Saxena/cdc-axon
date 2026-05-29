package core

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Srajan-Sanjay-Saxena/Exponential_Backoff/backoff"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
)

type Engine struct {
	source  engine_source.EngineSource
	backoff *backoff.Backoff
}

func New(src engine_source.EngineSource) *Engine {
	return &Engine{
		source:  src,
		backoff: backoff.New(1*time.Second, 60*time.Second, 5*time.Minute),
	}
}

func (e *Engine) Start(ctx context.Context) error {
	for {
		e.backoff.RecordStart()

		if err := e.run(ctx); err != nil {
			if ctx.Err() != nil {
				log.Println("cdc-axon: shutting down")
				return nil
			}
			log.Printf("cdc-axon: error: %v", err)
			if err := e.backoff.Wait(ctx); err != nil {
				log.Println("cdc-axon: shutting down")
				return nil
			}
			continue
		}

		return nil
	}
}

func (e *Engine) run(ctx context.Context) error {
	if err := e.source.DBConnect(ctx); err != nil {
		return fmt.Errorf("source connect: %w", err)
	}
	defer e.source.Close(ctx)

	events, err := e.source.CaptureEvents(ctx)
	if err != nil {
		return fmt.Errorf("source capture events: %w", err)
	}

	producers, err := e.source.GetProducers()
	if err != nil {
		return fmt.Errorf("get producers: %w", err)
	}

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

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("event channel closed")
			}

			if err := e.publishAll(ctx, producers, ev); err != nil {
				return fmt.Errorf("publish failed: %w", err)
			}

			if err := e.source.Ack(ctx); err != nil {
				return fmt.Errorf("ack failed: %w", err)
			}
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

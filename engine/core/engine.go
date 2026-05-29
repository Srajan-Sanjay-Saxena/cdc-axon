package core

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/Exponential_Backoff/backoff"
)

type Engine struct {
	source   engine_source.EngineSource
	backoff  *backoff.Backoff
}   

func New(src engine_source.EngineSource) *Engine {
	return &Engine{
		source:   src,
		backoff:  backoff.New(1*time.Second, 60*time.Second, 5*time.Minute),
	}
}

func (e *Engine) Start(ctx context.Context) error {
	for {
		e.backoff.RecordStart()

		if err := e.run(ctx); err != nil {
			if ctx.Err() != nil {
				log.Println("cdcrelay: shutting down")
				return nil
			}
			log.Printf("cdcrelay: error: %v", err)
			if err := e.backoff.Wait(ctx); err != nil {
				log.Println("cdcrelay: shutting down")
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

	producer, err := e.source.GetProducer()
	if err != nil {
		return fmt.Errorf("get producer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-events:
			if !ok {
				return fmt.Errorf("event channel closed")
			}

			if err := producer.Publish(ctx, event); err != nil {
				return fmt.Errorf("publish failed: %w", err)
			}

			if err := e.source.Ack(ctx); err != nil {
				return fmt.Errorf("ack failed: %w", err)
			}
		}
	}
}

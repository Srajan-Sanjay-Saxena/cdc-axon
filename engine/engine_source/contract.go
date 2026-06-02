package engine_source

import (
	"context"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
)

type EngineSource interface {
	DBConnect(ctx context.Context) error
	CaptureEvents(ctx context.Context) (<-chan event.Event, error)
	Ack(ctx context.Context) error
	Close(ctx context.Context) error
	GetProducers() ([]Producer, error)
}

type Producer interface {
	Connect(ctx context.Context) error
	Publish(ctx context.Context, event event.Event) error
	Close() error
}


type PersistenceStore interface {
	Connect(ctx context.Context) error
	CloseStoreConnection(ctx context.Context) error
	Save(ctx context.Context, key string, value []byte) error
	Load(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type DeduplicationStore interface {
	SaveWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Exists(ctx context.Context, key string) (bool, error)
}

type Transform func(ctx context.Context, e event.Event) (event.Event, bool, error)


package snapshot

import (
	"context"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
)

type Mode int

const (
	Never Mode = iota
	Initial
	Always
)

type SnapshotSource interface {
	SyncSnapshot(ctx context.Context, table string, primaryKey string, batchSize int, mode Mode, ch chan<- event.Event) error
}

package snapshot

import (
	"context"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/logger"
)

type SnapshotEngine struct {
	table      string
	primaryKey string
	batchSize  int
	mode       Mode
	source     SnapshotSource
	log        *logger.Logger
}

func NewEngine(source SnapshotSource, mode Mode) *SnapshotEngine {
	return &SnapshotEngine{
		batchSize: 10000,
		mode:      mode,
		source:    source,
		log:       logger.Default(),
	}
}

func (e *SnapshotEngine) Table(table string) *SnapshotEngine {
	e.table = table
	return e
}

func (e *SnapshotEngine) PrimaryKey(pk string) *SnapshotEngine {
	e.primaryKey = pk
	return e
}

func (e *SnapshotEngine) BatchSize(size int) *SnapshotEngine {
	e.batchSize = size
	return e
}

func (e *SnapshotEngine) SetLogger(l *logger.Logger) *SnapshotEngine {
	e.log = l
	return e
}

func (e *SnapshotEngine) SyncSnapshot(ctx context.Context, ch chan<- event.Event) error {
	if e.mode == Never {
		return nil
	}

	e.log.Info("snapshot: starting", "table", e.table, "batch_size", e.batchSize, "mode", e.mode)

	err := e.source.SyncSnapshot(ctx, e.table, e.primaryKey, e.batchSize, e.mode, ch)
	if err != nil {
		return err
	}

	e.log.Info("snapshot: complete", "table", e.table)
	return nil
}

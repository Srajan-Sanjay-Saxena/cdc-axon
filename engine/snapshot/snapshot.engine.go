package snapshot

import (
	"context"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/logger"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/metrics"
)

type SnapshotEngine struct {
	table      string
	primaryKey string
	batchSize  int
	mode       Mode
	source     SnapshotSource
	log        *logger.Logger
	metrics    metrics.Metrics
}

func NewEngine(source SnapshotSource, mode Mode) *SnapshotEngine {
	return &SnapshotEngine{
		batchSize: 10000,
		mode:      mode,
		source:    source,
		log:       logger.Default(),
	}
}

func (e *SnapshotEngine) SetMetrics(m metrics.Metrics) *SnapshotEngine {
	e.metrics = m
	return e
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

	// wrap channel to track progress
	tracked := make(chan event.Event)
	var rowsProcessed int64

	go func() {
		for ev := range tracked {
			rowsProcessed++
			if e.metrics != nil && rowsProcessed%int64(e.batchSize) == 0 {
				e.metrics.SnapshotProgress(ctx, e.table, rowsProcessed)
			}
			ch <- ev
		}
	}()

	err := e.source.SyncSnapshot(ctx, e.table, e.primaryKey, e.batchSize, e.mode, tracked)
	close(tracked)

	if err != nil {
		return err
	}

	if e.metrics != nil {
		e.metrics.SnapshotProgress(ctx, e.table, rowsProcessed)
	}
	e.log.Info("snapshot: complete", "table", e.table, "rows", rowsProcessed)
	return nil
}

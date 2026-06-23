package metrics

import (
	"context"
	"time"
)

type Metrics interface {
	EventCaptured(ctx context.Context, source string, operation string)
	EventDropped(ctx context.Context, source string, reason string)
	EventPublished(ctx context.Context, producer string, duration time.Duration)
	PublishFailed(ctx context.Context, producer string)
	AckCompleted(ctx context.Context, source string)
	RetryTriggered(ctx context.Context)
	SnapshotProgress(ctx context.Context, table string, rowsProcessed int64)
}

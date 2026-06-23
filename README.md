# CDC-Axon

> CDC-Axon is a production-grade Go SDK for Change Data Capture. It taps directly into PostgreSQL's Write-Ahead Log and MongoDB's change streams, relaying outbox events to any message broker with at-least-once delivery, crash recovery, and pluggable persistence — so your distributed system never misses a beat.

[![Go Reference](https://pkg.go.dev/badge/github.com/Srajan-Sanjay-Saxena/cdc-axon.svg)](https://pkg.go.dev/github.com/Srajan-Sanjay-Saxena/cdc-axon)
[![Go Report Card](https://goreportcard.com/badge/github.com/Srajan-Sanjay-Saxena/cdc-axon)](https://goreportcard.com/report/github.com/Srajan-Sanjay-Saxena/cdc-axon)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)

---

## What is CDC-Axon?

In distributed systems, services need to react to database changes reliably. The outbox pattern solves dual-write problems — but you still need something to capture those outbox events and relay them downstream.

**CDC-Axon is that something.**

It sits between your database and your message broker, capturing change events at the wire level — directly from PostgreSQL's WAL or MongoDB's change streams — and transmitting them with guaranteed at-least-once delivery.

No polling. No triggers. No expensive enterprise tooling.

---

## Features

- **Full DML capture** — INSERT, UPDATE, and DELETE events captured from PostgreSQL WAL
- **PostgreSQL WAL** — logical replication via `pgoutput`, no triggers, no polling
- **MongoDB Change Streams** — oplog-based, resume token persistence across restarts
- **At-least-once delivery** — LSN/resume token only advances after successful publish
- **Crash recovery** — replication slot survives Postgres restarts, Redis-backed resume token for Mongo
- **Relation metadata persistence** — WAL relation messages persisted to survive fast reconnects
- **Multiple producers** — fan out a single event to multiple brokers concurrently
- **Concurrent publish** — all producers publish in parallel via goroutines with buffered error channel
- **Pluggable producer** — implement `Producer` interface for any message broker (RabbitMQ, Kafka, SQS, etc.)
- **Pluggable persistence** — implement `PersistenceStore` interface for any backend (Redis, in-memory, etc.)
- **Pluggable metrics** — implement `Metrics` interface for any observability stack (Prometheus, Datadog, CloudWatch, etc.)
- **Exponential backoff** — automatic retry with configurable backoff on failures
- **Clean interfaces** — engine, source, producer, and persistence are fully decoupled


---

## Installation

```bash
go get github.com/Srajan-Sanjay-Saxena/cdc-axon@v1.6.0
```

---

## Quick Start

> **Note:** The following are minimalistic examples to get you started. Production usage should include proper error handling, graceful shutdown, and persistence configuration.

### PostgreSQL → Single Producer

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
    pgConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
    pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    src := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
        URL:             "postgresql://user:pass@localhost:5432/mydb",
        SlotName:        "myslot",
        PublicationName: "mypub",
    }).AddProducer(myRabbitMQProducer)

    core.New(src).Start(ctx)
}
```

### PostgreSQL → Multiple Producers (Fan-out)

```go
// CDC-Axon publishes to all producers concurrently via goroutines.
// Ack (LSN advancement) only happens after ALL producers succeed.
src := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
    URL:             "postgresql://user:pass@localhost:5432/mydb",
    SlotName:        "myslot",
    PublicationName: "mypub",
}).
    AddProducer(rabbitMQProducer).   // publishes concurrently
    AddProducer(kafkaProducer).      // publishes concurrently
    AddProducer(sqsProducer)         // publishes concurrently

core.New(src).Start(ctx)
```

### MongoDB → Single Producer

```go
src := mongoSource.NewSource(&mongoConfig.MongoRelaySourceConfig{
    URI:            "mongodb://localhost:27017",
    Database:       "mydb",
    CollectionName: "outbox",
}).AddProducer(myProducer)

core.New(src).Start(ctx)
```

### MongoDB → Multiple Producers with Redis Persistence

```go
// Redis persistence saves the resume token after every successful publish.
// On restart, CDC-Axon resumes exactly from where it left off.
src := mongoSource.NewSource(&mongoConfig.MongoRelaySourceConfig{
    URI:            "mongodb://localhost:27017",
    Database:       "mydb",
    CollectionName: "outbox",
}).
    AddPersistanceStore(myRedisStore).
    AddProducer(rabbitMQProducer).
    AddProducer(kafkaProducer)

core.New(src).Start(ctx)
```

### PostgreSQL with Redis Persistence (Crash Recovery)

```go
// Redis persistence saves WAL relation metadata.
// If the relay crashes and reconnects before wal_sender_timeout,
// relation metadata is loaded from Redis — no RelationMessage needed.
src := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
    URL:             "postgresql://user:pass@localhost:5432/mydb",
    SlotName:        "myslot",
    PublicationName: "mypub",
}).
    AddPersistanceStore(myRedisStore).
    AddProducer(myProducer)

core.New(src).Start(ctx)
```

### Running Both Sources Simultaneously

```go
// Run Postgres and Mongo engines concurrently — each with their own producers.
pgSrc := pgSource.NewSource(pgCfg).
    AddPersistanceStore(redisStore).
    AddProducer(rabbitMQProducer)

mongoSrc := mongoSource.NewSource(mongoCfg).
    AddPersistanceStore(redisStore).
    AddProducer(kafkaProducer)

go core.New(pgSrc).Start(ctx)
go core.New(mongoSrc).Start(ctx)

<-ctx.Done()
```

---

## Implementing Producer

> **Note:** This is a minimalistic example. Production implementations should handle reconnection, channel recovery, and publisher confirms.

```go
type MyProducer struct{}

func (p *MyProducer) Connect(ctx context.Context) error {
    // establish connection to your message broker
    return nil
}

func (p *MyProducer) Publish(ctx context.Context, e event.Event) error {
    // serialize and publish e to your broker
    // return error to trigger retry — Ack will NOT be called on failure
    return nil
}

func (p *MyProducer) Close() error {
    // close broker connection
    return nil
}
```

---

## Implementing PersistenceStore

> **Note:** This is a minimalistic example using Redis. You can implement any backend — in-memory, file-based, DynamoDB, etc.

```go
type RedisStore struct {
    client *redis.Client
}

func (s *RedisStore) Connect(ctx context.Context) error {
    return s.client.Ping(ctx).Err()
}

func (s *RedisStore) CloseStoreConnection(ctx context.Context) error {
    return s.client.Close()
}

func (s *RedisStore) Save(ctx context.Context, key string, value []byte) error {
    return s.client.Set(ctx, key, value, 0).Err()
}

func (s *RedisStore) Load(ctx context.Context, key string) ([]byte, error) {
    val, err := s.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, nil
    }
    return val, err
}

func (s *RedisStore) Delete(ctx context.Context, key string) error {
    return s.client.Del(ctx, key).Err()
}

func (s *RedisStore) Keys(ctx context.Context, pattern string) ([]string, error) {
    return s.client.Keys(ctx, pattern).Result()
}
```

---

## Outbox Table Schema

### PostgreSQL
```sql
CREATE TABLE outbox (
    id          TEXT PRIMARY KEY,
    event_type  TEXT NOT NULL,
    payload     JSONB
);

-- Required for full row data on DELETE events
ALTER TABLE outbox REPLICA IDENTITY FULL;

CREATE PUBLICATION mypub FOR TABLE outbox;
```

> **Note:** Without `REPLICA IDENTITY FULL`, DELETE events will only contain primary key columns. If you need the full row payload on deletes, set replica identity to FULL.

### MongoDB
```json
{
    "_id":        "uuid",
    "event_type": "ORDER_CREATED",
    "payload":    { "orderId": "123", "amount": 99.99 }
}
```

---

## Event Structure

Every event captured by CDC-Axon — regardless of source — is normalized into a single `Event` struct:

```go
type Event struct {
    ID        string          // unique identifier from the outbox row
    Source    string          // table or collection name
    Operation OperationType   // insert, update, delete
    EventType string          // domain event type e.g. ORDER_CREATED
    Payload   json.RawMessage // raw JSON payload
}
```

This means your `Producer` implementation receives the same `Event` shape whether the source is Postgres or MongoDB — making it trivial to build broker-agnostic consumers.

---

## Observability

CDC-Axon provides a pluggable `Metrics` interface for real-time observability. Implement it for any backend — Prometheus, Datadog, CloudWatch, or all of them at once.

### Metrics Interface

```go
type Metrics interface {
    EventCaptured(ctx context.Context, source string, operation string)
    EventDropped(ctx context.Context, source string, reason string)
    EventPublished(ctx context.Context, producer string, duration time.Duration)
    PublishFailed(ctx context.Context, producer string)
    AckCompleted(ctx context.Context, source string)
    RetryTriggered(ctx context.Context)
    SnapshotProgress(ctx context.Context, table string, rowsProcessed int64)
}
```

If you don't call `SetMetrics`, the engine runs with zero overhead — no metrics are collected.

### Prometheus Example

```go
package main

import (
    "context"
    "net/http"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"

    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/metrics"
    pgConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
    pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
)

// PrometheusMetrics implements metrics.Metrics for Prometheus.
type PrometheusMetrics struct {
    eventsCaptured  *prometheus.CounterVec
    eventsDropped   *prometheus.CounterVec
    eventsPublished *prometheus.CounterVec
    publishFailed   *prometheus.CounterVec
    publishDuration *prometheus.HistogramVec
    acksCompleted   *prometheus.CounterVec
    retries         prometheus.Counter
    snapshotRows    *prometheus.GaugeVec
}

func NewPrometheusMetrics() *PrometheusMetrics {
    m := &PrometheusMetrics{
        eventsCaptured: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "cdc_axon_events_captured_total",
            Help: "Total events captured from source",
        }, []string{"source", "operation"}),

        eventsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "cdc_axon_events_dropped_total",
            Help: "Total events dropped by transforms",
        }, []string{"source", "reason"}),

        eventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "cdc_axon_events_published_total",
            Help: "Total events successfully published",
        }, []string{"producer"}),

        publishFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "cdc_axon_publish_errors_total",
            Help: "Total publish failures",
        }, []string{"producer"}),

        publishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
            Name:    "cdc_axon_publish_duration_seconds",
            Help:    "Publish latency distribution",
            Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
        }, []string{"producer"}),

        acksCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "cdc_axon_acks_total",
            Help: "Total successful acks to source",
        }, []string{"source"}),

        retries: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "cdc_axon_retries_total",
            Help: "Total engine retries triggered",
        }),

        snapshotRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "cdc_axon_snapshot_rows_processed",
            Help: "Snapshot progress by table",
        }, []string{"table"}),
    }

    prometheus.MustRegister(
        m.eventsCaptured, m.eventsDropped, m.eventsPublished,
        m.publishFailed, m.publishDuration, m.acksCompleted,
        m.retries, m.snapshotRows,
    )
    return m
}

func (m *PrometheusMetrics) EventCaptured(_ context.Context, source, operation string) {
    m.eventsCaptured.WithLabelValues(source, operation).Inc()
}
func (m *PrometheusMetrics) EventDropped(_ context.Context, source, reason string) {
    m.eventsDropped.WithLabelValues(source, reason).Inc()
}
func (m *PrometheusMetrics) EventPublished(_ context.Context, producer string, duration time.Duration) {
    m.eventsPublished.WithLabelValues(producer).Inc()
    m.publishDuration.WithLabelValues(producer).Observe(duration.Seconds())
}
func (m *PrometheusMetrics) PublishFailed(_ context.Context, producer string) {
    m.publishFailed.WithLabelValues(producer).Inc()
}
func (m *PrometheusMetrics) AckCompleted(_ context.Context, source string) {
    m.acksCompleted.WithLabelValues(source).Inc()
}
func (m *PrometheusMetrics) RetryTriggered(_ context.Context) {
    m.retries.Inc()
}
func (m *PrometheusMetrics) SnapshotProgress(_ context.Context, table string, rows int64) {
    m.snapshotRows.WithLabelValues(table).Set(float64(rows))
}

var _ metrics.Metrics = (*PrometheusMetrics)(nil)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    // expose /metrics for Prometheus to scrape
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        http.ListenAndServe(":9090", nil)
    }()

    src := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
        URL:             "postgresql://user:pass@localhost:5432/mydb",
        SlotName:        "myslot",
        PublicationName: "mypub",
    }).AddProducer(myRabbitMQProducer)

    core.New(src).
        SetMetrics(NewPrometheusMetrics()).
        Start(ctx)
}
```

Prometheus scrapes `:9090/metrics` and you get:
```
cdc_axon_events_captured_total{source="outbox", operation="insert"}     48721
cdc_axon_publish_duration_seconds{producer="producer_0", quantile="0.99"}  0.011
cdc_axon_retries_total                                                  0
```

### Structured Logging with Loki

CDC-Axon uses Go's `log/slog` for structured logging. To ship logs to Loki (for Grafana), configure a JSON logger and let Promtail/Grafana Agent collect from stdout:

```go
package main

import (
    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/logger"
    pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
)

func main() {
    // Production mode = JSON output, WARN level and above
    // Promtail/Grafana Agent tails stdout → ships to Loki → queryable in Grafana
    log := logger.New(logger.Production)

    src := pgSource.NewSource(cfg).AddProducer(myProducer)

    core.New(src).
        SetLogger(log).
        SetMetrics(NewPrometheusMetrics()).
        Start(ctx)
}
```

Production JSON output (what Loki ingests):
```json
{"time":"2025-01-15T03:17:42Z","level":"ERROR","msg":"cdc-axon: run error","error":"publish failed: connection refused"}
{"time":"2025-01-15T03:17:43Z","level":"WARN","msg":"postgres source: slot still active, retrying","attempt":3}
```

Query in Grafana Loki:
```
{job="cdc-axon"} |= "publish failed"
{job="cdc-axon"} | json | level="ERROR"
```

### Metrics + Logs Together

| What you need | Where to look |
|---|---|
| "Is the pipeline flowing?" | Prometheus → `cdc_axon_events_captured_total` rate |
| "What's the p99 latency?" | Prometheus → `cdc_axon_publish_duration_seconds` |
| "Why did event X fail?" | Loki → `{job="cdc-axon"} \|= "evt-X"` |
| "How many retries today?" | Prometheus → `cdc_axon_retries_total` |
| "What error caused the retry?" | Loki → `{job="cdc-axon"} \| json \| level="ERROR"` |

### Multiple Backends (Composite Metrics)

```go
// Send metrics to Prometheus AND Datadog simultaneously.
type CompositeMetrics struct {
    backends []metrics.Metrics
}

func (c *CompositeMetrics) EventCaptured(ctx context.Context, source, op string) {
    for _, b := range c.backends {
        b.EventCaptured(ctx, source, op)
    }
}
// ... same pattern for all other methods

// Usage:
core.New(src).
    SetMetrics(&CompositeMetrics{backends: []metrics.Metrics{
        NewPrometheusMetrics(),
        NewDatadogMetrics(),
    }}).
    Start(ctx)
```



---

## Built With

- [`jackc/pglogrepl`](https://github.com/jackc/pglogrepl) — PostgreSQL logical replication
- [`jackc/pgx`](https://github.com/jackc/pgx) — PostgreSQL driver
- [`mongo-driver`](https://github.com/mongodb/mongo-go-driver) — MongoDB driver
- [`go-redis`](https://github.com/redis/go-redis) — Redis client
- [`Exponential_Backoff`](https://github.com/Srajan-Sanjay-Saxena/Exponential_Backoff) — backoff implementation
- [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) — integration testing

---

## Author

**Srajan Saxena** — IIT (BHU) Varanasi, Computer Science
Distributed systems enthusiast since 10th grade. Built CDC-Axon to understand how enterprise systems really talk to each other at the wire level.

---

## License

Proprietary — Copyright © 2025 Srajan Saxena. All Rights Reserved.
See [LICENSE](LICENSE) for full terms.

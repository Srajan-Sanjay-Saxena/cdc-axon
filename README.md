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
- **Exponential backoff** — automatic retry with configurable backoff on failures
- **Clean interfaces** — engine, source, producer, and persistence are fully decoupled

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CDC-Axon Engine                       │
│                                                              │
│   EngineSource              Producers (concurrent)           │
│   ┌──────────┐             ┌──────────┐  ┌──────────┐       │
│   │ DBConnect│             │ Producer │  │ Producer │  ...  │
│   │ Capture  │──event──┬──▶│ Publish  │  │ Publish  │       │
│   │ Events   │         │   └──────────┘  └──────────┘       │
│   │ Ack      │         │   (goroutine)   (goroutine)         │
│   │ Close    │         └── WaitGroup — Ack after all done    │
│   └──────────┘                                               │
│        │                                                     │
│   PersistenceStore                                           │
│   ┌──────────┐                                               │
│   │ Save     │  (relation metadata / resume token)           │
│   │ Load     │                                               │
│   │ Delete   │                                               │
│   │ Keys     │                                               │
│   └──────────┘                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Installation

```bash
go get github.com/Srajan-Sanjay-Saxena/cdc-axon@v1.1.0
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

## How At-Least-Once Delivery Works

```
CaptureEvents goroutine
    │
    ├── WAL/oplog event received
    ├── event sent to channel
    │
Engine loop
    ├── receives event from channel
    ├── publishAll(producers) — all producers publish concurrently
    │       ├── goroutine → Producer1.Publish()
    │       ├── goroutine → Producer2.Publish()
    │       └── WaitGroup.Wait() — blocks until ALL succeed
    │
    ├── ALL succeeded → Ack()
    │       ├── Postgres: SendStandbyStatusUpdate → LSN advances
    │       └── Mongo: save resume token to Redis
    │
    └── ANY failed → return error → engine retries with backoff
                     LSN/token NOT advanced → DB resends event
```

---

## Project Structure

```
cdc-axon/
├── engine/
│   ├── core/           # Engine — orchestration, backoff, retry, concurrent publish
│   ├── engine_source/  # Contracts — EngineSource, Producer, PersistenceStore
│   └── event/          # Event struct and OperationType
└── source/
    ├── postgres/
    │   ├── config/     # PgRelaySourceConfig
    │   ├── source/     # PgRelaySource — DBConnect, CaptureEvents, Ack
    │   └── walHandlers/# WAL message parsing, relation persistence
    └── mongo/
        ├── config/     # MongoRelaySourceConfig
        ├── source/     # MongoRelaySource — DBConnect, CaptureEvents, Ack
        └── stream/     # Change stream opening
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

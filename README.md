# CDC-Axon

> CDC-Axon is a production-grade Go SDK for Change Data Capture. It taps directly into PostgreSQL's Write-Ahead Log and MongoDB's change streams, relaying outbox events to any message broker with at-least-once delivery, crash recovery, and pluggable persistence — so your distributed system never misses a beat.

---

## What is CDC-Axon?

In distributed systems, services need to react to database changes reliably. The outbox pattern solves dual-write problems — but you still need something to capture those outbox events and relay them downstream.

CDC-Axon is that something.

It sits between your database and your message broker, capturing change events at the wire level — directly from PostgreSQL's WAL or MongoDB's change streams — and transmitting them with guaranteed at-least-once delivery.

---

## Features

- **PostgreSQL WAL** — logical replication via `pgoutput`, no triggers, no polling
- **MongoDB Change Streams** — oplog-based, resume token persistence across restarts
- **At-least-once delivery** — LSN/resume token only advances after successful publish
- **Crash recovery** — replication slot survives Postgres restarts, Redis-backed resume token for Mongo
- **Relation metadata persistence** — WAL relation messages persisted to survive fast reconnects
- **Pluggable producer** — implement `Producer` interface for any message broker (RabbitMQ, Kafka, SQS, etc.)
- **Pluggable persistence** — implement `PersistenceStore` interface for any backend (Redis, in-memory, etc.)
- **Exponential backoff** — automatic retry with configurable backoff on failures
- **Clean interfaces** — engine, source, producer, and persistence are fully decoupled

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     CDC-Axon Engine                  │
│                                                      │
│   EngineSource          Producer                     │
│   ┌──────────┐         ┌──────────┐                  │
│   │ DBConnect│         │ Connect  │                  │
│   │ Capture  │──event──▶ Publish  │                  │
│   │ Events   │         │ Close    │                  │
│   │ Ack      │         └──────────┘                  │
│   │ Close    │                                       │
│   └──────────┘                                       │
│        │                                             │
│   PersistenceStore                                   │
│   ┌──────────┐                                       │
│   │ Save     │  (relation metadata / resume token)   │
│   │ Load     │                                       │
│   │ Delete   │                                       │
│   │ Keys     │                                       │
│   └──────────┘                                       │
└─────────────────────────────────────────────────────┘
```

---

## Installation

```bash
go get github.com/Srajan-Sanjay-Saxena/cdc-axon
```

---

## Quick Start

### PostgreSQL

```go
import (
    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
    pgConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
    pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
)

src := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
    URL:             "postgresql://user:pass@localhost:5432/mydb?replication=database",
    SlotName:        "myslot",
    PublicationName: "mypub",
}).AddProducer(myProducer)

engine := core.New(src)
engine.Start(ctx)
```

### MongoDB

```go
import (
    "github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
    mongoConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
    mongoSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/source"
)

src := mongoSource.NewSource(&mongoConfig.MongoRelaySourceConfig{
    URI:            "mongodb://localhost:27017",
    Database:       "mydb",
    CollectionName: "outbox",
}).AddPersistanceStore(myRedisStore).AddProducer(myProducer)

engine := core.New(src)
engine.Start(ctx)
```

---

## Implementing Producer

```go
type MyProducer struct{}

func (p *MyProducer) Connect(ctx context.Context) error { ... }
func (p *MyProducer) Publish(ctx context.Context, e event.Event) error { ... }
func (p *MyProducer) Close() error { ... }
```

## Implementing PersistenceStore

```go
type MyStore struct{}

func (s *MyStore) Connect(ctx context.Context) error { ... }
func (s *MyStore) CloseStoreConnection(ctx context.Context) error { ... }
func (s *MyStore) Save(ctx context.Context, key string, value []byte) error { ... }
func (s *MyStore) Load(ctx context.Context, key string) ([]byte, error) { ... }
func (s *MyStore) Delete(ctx context.Context, key string) error { ... }
func (s *MyStore) Keys(ctx context.Context, pattern string) ([]string, error) { ... }
```

---

## Outbox Table Schema

### PostgreSQL
```sql
CREATE TABLE outbox (
    id          TEXT PRIMARY KEY,
    event_type  TEXT NOT NULL,
    operation   TEXT NOT NULL,
    payload     JSONB
);
CREATE PUBLICATION mypub FOR TABLE outbox;
```

### MongoDB
```json
{
    "_id":        "uuid",
    "event_type": "ORDER_CREATED",
    "payload":    {}
}
```

---

## Event Structure

```go
type Event struct {
    ID        string
    Source    string          // table or collection name
    Operation OperationType   // insert, update, delete
    EventType string
    Payload   json.RawMessage
}
```

---

## Project Structure

```
cdc-axon/
├── engine/
│   ├── core/           # Engine — orchestration, backoff, retry
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
- [`Exponential_Backoff`](https://github.com/Srajan-Sanjay-Saxena/Exponential_Backoff) — backoff implementation

---

## Author

**Srajan Saxena** — IIT (BHU) Varanasi, Computer Science  
Distributed systems enthusiast since 10th grade. Built CDC-Axon to understand how enterprise systems really talk to each other at the wire level.

---

## License

MIT

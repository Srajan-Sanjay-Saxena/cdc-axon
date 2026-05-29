package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/core"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/config"
	pgSource "github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/source"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcRabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dbName = "testdb"
	dbUser = "testuser"
	dbPass = "testpass"
)

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := tcPostgres.Run(ctx,
		"postgres:15-alpine",
		tcPostgres.WithDatabase(dbName),
		tcPostgres.WithUsername(dbUser),
		tcPostgres.WithPassword(dbPass),
		testcontainers.WithCmdArgs("-c", "wal_level=logical"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	connStr, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get postgres connection string: %v", err)
	}
	return connStr
}

func startRabbitMQ(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := tcRabbit.Run(ctx, "rabbitmq:3.12-management-alpine")
	if err != nil {
		t.Fatalf("failed to start rabbitmq: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	url, err := c.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("failed to get rabbitmq url: %v", err)
	}
	return url
}

func TestPgSource_ConnectAndSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startPostgres(t, ctx)

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}
	src := pgSource.NewSource(cfg).AddProducer(&mockProducer{})

	if err := src.DBConnect(ctx); err != nil {
		t.Fatalf("DBConnect failed: %v", err)
	}
	defer src.Close(ctx)

	pgxConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	defer pgxConn.Close(ctx)

	var plugin string
	err = pgxConn.QueryRow(ctx,
		"SELECT plugin FROM pg_replication_slots WHERE slot_name = $1", "myslot",
	).Scan(&plugin)
	if err != nil {
		t.Fatalf("slot not found: %v", err)
	}
	if plugin != "pgoutput" {
		t.Errorf("expected pgoutput, got %q", plugin)
	}
}

func TestPgSource_ConnectIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startPostgres(t, ctx)

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}
	src := pgSource.NewSource(cfg).AddProducer(&mockProducer{})

	if err := src.DBConnect(ctx); err != nil {
		t.Fatalf("first DBConnect failed: %v", err)
	}
	src.Close(ctx)

	if err := src.DBConnect(ctx); err != nil {
		t.Fatalf("second DBConnect should not fail: %v", err)
	}
	defer src.Close(ctx)
}

func TestPgSource_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startPostgres(t, ctx)
	amqpURL := startRabbitMQ(t, ctx)

	pgxConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	_, err = pgxConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			operation TEXT NOT NULL,
			payload JSONB
		);
		CREATE PUBLICATION mypub FOR TABLE outbox;
	`)
	pgxConn.Close(ctx)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	prod := &rabbitProducer{url: amqpURL}
	if err := prod.Connect(ctx); err != nil {
		t.Fatalf("producer connect failed: %v", err)
	}
	defer prod.Close()

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}
	src := pgSource.NewSource(cfg).AddProducer(prod)
	engine := core.New(src)

	engineCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go engine.Start(engineCtx)

	// wait for engine to connect and start replication
	time.Sleep(2 * time.Second)

	pgxConn2, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	_, err = pgxConn2.Exec(ctx,
		`INSERT INTO outbox (id, event_type, operation, payload) VALUES ($1, $2, $3, $4)`,
		"test-1", "ORDER_CREATED", "insert", `{"orderId": 123}`,
	)
	pgxConn2.Close(ctx)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	amqpConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("amqp dial failed: %v", err)
	}
	defer amqpConn.Close()

	ch, err := amqpConn.Channel()
	if err != nil {
		t.Fatalf("amqp channel failed: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume("outbox_events", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	select {
	case msg := <-msgs:
		var got event.Event
		if err := json.Unmarshal(msg.Body, &got); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if got.EventType != "ORDER_CREATED" {
			t.Errorf("expected ORDER_CREATED, got %q", got.EventType)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for message on queue")
	}
}

// mockProducer — in-memory producer for slot/connect tests
type mockProducer struct {
	events []event.Event
}

func (m *mockProducer) Connect(_ context.Context) error { return nil }
func (m *mockProducer) Publish(_ context.Context, e event.Event) error {
	m.events = append(m.events, e)
	return nil
}
func (m *mockProducer) Close() error { return nil }

var _ engine_source.Producer = (*mockProducer)(nil)

// rabbitProducer — real RabbitMQ producer for end-to-end test
type rabbitProducer struct {
	url  string
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (r *rabbitProducer) Connect(_ context.Context) error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	_, err = ch.QueueDeclare("outbox_events", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return err
	}
	r.conn = conn
	r.ch = ch
	return nil
}

func (r *rabbitProducer) Publish(_ context.Context, e event.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return r.ch.Publish("", "outbox_events", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

func (r *rabbitProducer) Close() error {
	if r.ch != nil {
		r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

var _ engine_source.Producer = (*rabbitProducer)(nil)

// startRedis starts a Redis testcontainer and returns the address
func startRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	c, err := tcRedis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get redis host: %v", err)
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get redis port: %v", err)
	}
	return redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
}

// redisStore implements engine_source.PersistenceStore backed by Redis
type redisStore struct {
	client *redis.Client
}

func (r *redisStore) Connect(_ context.Context) error { return nil }
func (r *redisStore) CloseStoreConnection(_ context.Context) error { return r.client.Close() }

func (r *redisStore) Save(ctx context.Context, key string, value []byte) error {
	return r.client.Set(ctx, key, value, 0).Err()
}

func (r *redisStore) Load(ctx context.Context, key string) ([]byte, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

func (r *redisStore) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *redisStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	return r.client.Keys(ctx, pattern).Result()
}

var _ engine_source.PersistenceStore = (*redisStore)(nil)

// TestPgSource_EndToEnd_WithRedisPersistence tests the full relay cycle with Redis
// as the persistence store for relation metadata. It simulates a crash by cancelling
// the engine context mid-way, then restarts and verifies the second event is delivered.
func TestPgSource_EndToEnd_WithRedisPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startPostgres(t, ctx)
	amqpURL := startRabbitMQ(t, ctx)
	rdb := startRedis(t, ctx)

	// setup outbox table and publication
	pgxConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	_, err = pgxConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS outbox (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			operation TEXT NOT NULL,
			payload JSONB
		);
		CREATE PUBLICATION mypub FOR TABLE outbox;
	`)
	pgxConn.Close(ctx)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	store := &redisStore{client: rdb}

	prod := &rabbitProducer{url: amqpURL}
	if err := prod.Connect(ctx); err != nil {
		t.Fatalf("producer connect failed: %v", err)
	}
	defer prod.Close()

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}

	// --- First engine run: process event-1, then simulate crash ---
	crashCtx, crash := context.WithCancel(ctx)

	src1 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod)
	engine1 := core.New(src1)
	go engine1.Start(crashCtx)
	time.Sleep(2 * time.Second)

	pgxConn2, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	_, err = pgxConn2.Exec(ctx,
		`INSERT INTO outbox (id, event_type, operation, payload) VALUES ($1, $2, $3, $4)`,
		"evt-1", "ORDER_CREATED", "insert", `{"orderId": 1}`,
	)
	if err != nil {
		t.Fatalf("insert evt-1 failed: %v", err)
	}

	// consume evt-1
	amqpConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("amqp dial failed: %v", err)
	}
	defer amqpConn.Close()
	ch, err := amqpConn.Channel()
	if err != nil {
		t.Fatalf("amqp channel failed: %v", err)
	}
	defer ch.Close()
	msgs, err := ch.Consume("outbox_events", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	select {
	case msg := <-msgs:
		var got event.Event
		json.Unmarshal(msg.Body, &got)
		if got.ID != "evt-1" {
			t.Errorf("expected evt-1, got %s", got.ID)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for evt-1")
	}

	// verify relation was persisted in Redis
	keys, err := rdb.Keys(ctx, "relation:*").Result()
	if err != nil || len(keys) == 0 {
		t.Fatal("expected relation metadata in Redis after first run")
	}
	t.Logf("relation keys in Redis: %v", keys)

	// simulate crash
	crash()
	time.Sleep(500 * time.Millisecond)

	// insert evt-2 while engine is down
	_, err = pgxConn2.Exec(ctx,
		`INSERT INTO outbox (id, event_type, operation, payload) VALUES ($1, $2, $3, $4)`,
		"evt-2", "ORDER_UPDATED", "update", `{"orderId": 2}`,
	)
	pgxConn2.Close(ctx)
	if err != nil {
		t.Fatalf("insert evt-2 failed: %v", err)
	}

	// --- Second engine run: should resume and deliver evt-2 ---
	// relation metadata loaded from Redis — no RelationMessage needed from Postgres
	src2 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod)
	engine2 := core.New(src2)

	engineCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()
	go engine2.Start(engineCtx2)
	time.Sleep(2 * time.Second)

	select {
	case msg := <-msgs:
		var got event.Event
		json.Unmarshal(msg.Body, &got)
		if got.ID != "evt-2" {
			t.Errorf("expected evt-2 after restart, got %s", got.ID)
		}
		if got.EventType != "ORDER_UPDATED" {
			t.Errorf("expected ORDER_UPDATED, got %s", got.EventType)
		}
		t.Logf("successfully received evt-2 after engine restart: %s", fmt.Sprintf("%+v", got))
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for evt-2 after restart")
	}
}

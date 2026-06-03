package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/snapshot"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
	pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startSnapshotPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()
	c, err := tcPostgres.Run(ctx,
		"postgres:15-alpine",
		tcPostgres.WithDatabase("testdb"),
		tcPostgres.WithUsername("testuser"),
		tcPostgres.WithPassword("testpass"),
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

func startSnapshotRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	c, err := tcRedis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379")
	return redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
}

func seedTable(t *testing.T, ctx context.Context, connStr string, rowCount int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		);
		CREATE PUBLICATION mypub FOR TABLE users;
	`)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	for i := 0; i < rowCount; i++ {
		_, err = conn.Exec(ctx,
			"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
			fmt.Sprintf("user-%04d", i),
			fmt.Sprintf("User %d", i),
			fmt.Sprintf("user%d@test.com", i),
		)
		if err != nil {
			t.Fatalf("insert row %d failed: %v", i, err)
		}
	}
}

// TestSnapshot_AlwaysMode verifies that snapshot in Always mode
// reads all existing rows and publishes them as SNAPSHOT events.
func TestSnapshot_AlwaysMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startSnapshotPostgres(t, ctx)

	// seed 50 rows
	seedTable(t, ctx, connStr, 50)

	// collect events via mock producer
	prod := &collectingProducer{}

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}
	src := pgSource.NewSource(cfg).AddProducer(prod)

	snapshotEng := snapshot.NewEngine(src, snapshot.Always).
		Table("users").
		PrimaryKey("id").
		BatchSize(10)

	engineCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	engine := core.New(src).BindSnapshotEngine(snapshotEng)
	go engine.Start(engineCtx)

	// wait for snapshot to complete
	time.Sleep(5 * time.Second)
	cancel()

	// verify all 50 rows were published
	prod.mu.Lock()
	defer prod.mu.Unlock()

	snapshotEvents := 0
	for _, ev := range prod.events {
		if ev.EventType == "SNAPSHOT" {
			snapshotEvents++
		}
	}

	if snapshotEvents != 50 {
		t.Errorf("expected 50 snapshot events, got %d", snapshotEvents)
	}

	// verify event structure
	if len(prod.events) > 0 {
		first := prod.events[0]
		if first.Operation != event.INSERT {
			t.Errorf("expected Operation=INSERT, got %s", first.Operation)
		}
		if first.Source != "users" {
			t.Errorf("expected Source=users, got %s", first.Source)
		}
		var payload map[string]interface{}
		json.Unmarshal(first.Payload, &payload)
		if payload["name"] == nil {
			t.Error("expected payload to contain 'name' field")
		}
		if payload["email"] == nil {
			t.Error("expected payload to contain 'email' field")
		}
	}

	t.Logf("snapshot delivered %d events successfully", snapshotEvents)
}

// TestSnapshot_InitialMode_SkipsOnSecondRun verifies that Initial mode
// runs snapshot on first startup and skips on second startup.
func TestSnapshot_InitialMode_SkipsOnSecondRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startSnapshotPostgres(t, ctx)
	rdb := startSnapshotRedis(t, ctx)

	seedTable(t, ctx, connStr, 20)

	store := &snapshotRedisStore{client: rdb}

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}

	// --- First run: snapshot should execute ---
	prod1 := &collectingProducer{}
	src1 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod1)

	snapshotEng1 := snapshot.NewEngine(src1, snapshot.Initial).
		Table("users").
		PrimaryKey("id").
		BatchSize(10)

	engineCtx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	go core.New(src1).BindSnapshotEngine(snapshotEng1).Start(engineCtx1)
	time.Sleep(5 * time.Second)
	cancel1()

	prod1.mu.Lock()
	firstRunCount := len(prod1.events)
	prod1.mu.Unlock()

	if firstRunCount != 20 {
		t.Errorf("first run: expected 20 events, got %d", firstRunCount)
	}

	// verify "snapshot:done" marker saved
	data, err := rdb.Get(ctx, "snapshot:done:users").Bytes()
	if err != nil || len(data) == 0 {
		t.Fatal("expected snapshot:done:users marker in Redis")
	}

	// --- Second run: snapshot should be skipped ---
	prod2 := &collectingProducer{}
	src2 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod2)

	snapshotEng2 := snapshot.NewEngine(src2, snapshot.Initial).
		Table("users").
		PrimaryKey("id").
		BatchSize(10)

	engineCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	go core.New(src2).BindSnapshotEngine(snapshotEng2).Start(engineCtx2)
	time.Sleep(5 * time.Second)
	cancel2()

	prod2.mu.Lock()
	secondRunCount := 0
	for _, ev := range prod2.events {
		if ev.EventType == "SNAPSHOT" {
			secondRunCount++
		}
	}
	prod2.mu.Unlock()

	if secondRunCount != 0 {
		t.Errorf("second run: expected 0 snapshot events (should skip), got %d", secondRunCount)
	}

	t.Logf("Initial mode: first run=%d events, second run=%d events (skipped)", firstRunCount, secondRunCount)
}

// TestSnapshot_ResumeAfterCrash verifies that snapshot resumes from
// the last processed key after a crash mid-snapshot.
func TestSnapshot_ResumeAfterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startSnapshotPostgres(t, ctx)
	rdb := startSnapshotRedis(t, ctx)

	// seed 100 rows
	seedTable(t, ctx, connStr, 100)

	store := &snapshotRedisStore{client: rdb}

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}

	// --- First run: crash after processing some rows ---
	prod1 := &crashingProducer{crashAfter: 30}
	src1 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod1)

	snapshotEng1 := snapshot.NewEngine(src1, snapshot.Always).
		Table("users").
		PrimaryKey("id").
		BatchSize(10)

	engineCtx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	go core.New(src1).BindSnapshotEngine(snapshotEng1).Start(engineCtx1)
	time.Sleep(5 * time.Second)
	cancel1()

	// verify cursor was saved in Redis
	cursor, err := rdb.Get(ctx, "snapshot:cursor:users").Bytes()
	if err != nil || len(cursor) == 0 {
		t.Fatal("expected snapshot:cursor:users in Redis after partial run")
	}
	t.Logf("cursor saved at: %s", string(cursor))

	// --- Second run: should resume from cursor ---
	prod2 := &collectingProducer{}
	src2 := pgSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod2)

	snapshotEng2 := snapshot.NewEngine(src2, snapshot.Always).
		Table("users").
		PrimaryKey("id").
		BatchSize(10)

	engineCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	go core.New(src2).BindSnapshotEngine(snapshotEng2).Start(engineCtx2)
	time.Sleep(5 * time.Second)
	cancel2()

	prod2.mu.Lock()
	resumedCount := 0
	for _, ev := range prod2.events {
		if ev.EventType == "SNAPSHOT" {
			resumedCount++
		}
	}
	prod2.mu.Unlock()

	// should have processed remaining rows (100 - 30 = 70)
	// might be slightly off depending on batch boundaries
	if resumedCount < 60 || resumedCount > 75 {
		t.Errorf("expected ~70 events on resume, got %d", resumedCount)
	}

	// verify cursor was cleaned up after completion
	_, err = rdb.Get(ctx, "snapshot:cursor:users").Bytes()
	if err != redis.Nil {
		t.Error("expected snapshot:cursor:users to be deleted after completion")
	}

	t.Logf("resume: processed %d remaining events after crash", resumedCount)
}

// TestSnapshot_InitialMode_NoPersistorFails verifies that Initial mode
// without a PersistenceStore returns an error.
func TestSnapshot_InitialMode_NoPersistorFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	connStr := startSnapshotPostgres(t, ctx)

	seedTable(t, ctx, connStr, 5)

	cfg := &config.PgRelaySourceConfig{
		URL:             connStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}

	// No persistence store added
	prod := &collectingProducer{}
	src := pgSource.NewSource(cfg).AddProducer(prod)

	snapshotEng := snapshot.NewEngine(src, snapshot.Initial).
		Table("users").
		PrimaryKey("id")

	engineCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	engine := core.New(src).BindSnapshotEngine(snapshotEng)
	go engine.Start(engineCtx)
	time.Sleep(3 * time.Second)

	// should have 0 events (snapshot failed due to no persistor)
	prod.mu.Lock()
	count := len(prod.events)
	prod.mu.Unlock()

	if count != 0 {
		t.Errorf("expected 0 events (snapshot should fail without persistor), got %d", count)
	}
}

// --- Test helpers ---

// collectingProducer collects all published events in memory
type collectingProducer struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *collectingProducer) Connect(_ context.Context) error { return nil }
func (p *collectingProducer) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
	return nil
}
func (p *collectingProducer) Close() error { return nil }

var _ engine_source.Producer = (*collectingProducer)(nil)

// crashingProducer fails after N events to simulate mid-snapshot crash
type crashingProducer struct {
	mu         sync.Mutex
	count      int
	crashAfter int
}

func (p *crashingProducer) Connect(_ context.Context) error { return nil }
func (p *crashingProducer) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	if p.count > p.crashAfter {
		return fmt.Errorf("simulated crash after %d events", p.crashAfter)
	}
	return nil
}
func (p *crashingProducer) Close() error { return nil }

var _ engine_source.Producer = (*crashingProducer)(nil)

// snapshotRedisStore implements engine_source.PersistenceStore
type snapshotRedisStore struct {
	client *redis.Client
}

func (r *snapshotRedisStore) Connect(_ context.Context) error              { return nil }
func (r *snapshotRedisStore) CloseStoreConnection(_ context.Context) error { return r.client.Close() }

func (r *snapshotRedisStore) Save(ctx context.Context, key string, value []byte) error {
	return r.client.Set(ctx, key, value, 0).Err()
}

func (r *snapshotRedisStore) Load(ctx context.Context, key string) ([]byte, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

func (r *snapshotRedisStore) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

var _ engine_source.PersistenceStore = (*snapshotRedisStore)(nil)

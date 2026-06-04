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
	mongoConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
	mongoSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/source"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func seedMongoCollection(t *testing.T, ctx context.Context, uri string, rowCount int) {
	t.Helper()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(ctx)

	col := client.Database("testdb").Collection("users")
	for i := 0; i < rowCount; i++ {
		_, err := col.InsertOne(ctx, bson.M{
			"_id":   fmt.Sprintf("user-%04d", i),
			"name":  fmt.Sprintf("User %d", i),
			"email": fmt.Sprintf("user%d@test.com", i),
		})
		if err != nil {
			t.Fatalf("insert doc %d failed: %v", i, err)
		}
	}
}

// TestMongoSnapshot_AlwaysMode verifies that snapshot in Always mode
// reads all existing documents and publishes them as SNAPSHOT events.
func TestMongoSnapshot_AlwaysMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)

	seedMongoCollection(t, ctx, uri, 50)

	prod := &snapshotCollectingProducer{}

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "users",
	}
	src := mongoSource.NewSource(cfg).AddProducer(prod)

	snapshotEng := snapshot.NewEngine(src, snapshot.Always).
		Table("users").
		PrimaryKey("_id").
		BatchSize(10)

	engineCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	engine := core.New(src).BindSnapshotEngine(snapshotEng)
	go engine.Start(engineCtx)

	time.Sleep(5 * time.Second)
	cancel()

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

	t.Logf("mongo snapshot delivered %d events successfully", snapshotEvents)
}

// TestMongoSnapshot_InitialMode_SkipsOnSecondRun verifies that Initial mode
// runs snapshot on first startup and skips on second startup.
func TestMongoSnapshot_InitialMode_SkipsOnSecondRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)
	rdb := startRedis(t, ctx)

	seedMongoCollection(t, ctx, uri, 20)

	store := &redisStore{client: rdb}

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "users",
	}

	// --- First run: snapshot should execute ---
	prod1 := &snapshotCollectingProducer{}
	src1 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod1)

	snapshotEng1 := snapshot.NewEngine(src1, snapshot.Initial).
		Table("users").
		PrimaryKey("_id").
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
	prod2 := &snapshotCollectingProducer{}
	src2 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod2)

	snapshotEng2 := snapshot.NewEngine(src2, snapshot.Initial).
		Table("users").
		PrimaryKey("_id").
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

	t.Logf("mongo Initial mode: first run=%d events, second run=%d events (skipped)", firstRunCount, secondRunCount)
}

// TestMongoSnapshot_ResumeAfterCrash verifies that snapshot resumes from
// the last processed key after a crash mid-snapshot.
func TestMongoSnapshot_ResumeAfterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)
	rdb := startRedis(t, ctx)

	seedMongoCollection(t, ctx, uri, 100)

	store := &redisStore{client: rdb}

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "users",
	}

	// --- First run: crash after processing some docs ---
	prod1 := &snapshotCrashingProducer{crashAfter: 30}
	src1 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod1)

	snapshotEng1 := snapshot.NewEngine(src1, snapshot.Always).
		Table("users").
		PrimaryKey("_id").
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
	t.Logf("mongo cursor saved at: %s", string(cursor))

	// --- Second run: should resume from cursor ---
	prod2 := &snapshotCollectingProducer{}
	src2 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod2)

	snapshotEng2 := snapshot.NewEngine(src2, snapshot.Always).
		Table("users").
		PrimaryKey("_id").
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

	// should have processed remaining docs (100 - 30 = 70)
	if resumedCount < 60 || resumedCount > 75 {
		t.Errorf("expected ~70 events on resume, got %d", resumedCount)
	}

	// verify cursor was cleaned up after completion
	_, err = rdb.Get(ctx, "snapshot:cursor:users").Bytes()
	if err != redis.Nil {
		t.Error("expected snapshot:cursor:users to be deleted after completion")
	}

	t.Logf("mongo resume: processed %d remaining events after crash", resumedCount)
}

// --- Test helpers ---

type snapshotCollectingProducer struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *snapshotCollectingProducer) Connect(_ context.Context) error { return nil }
func (p *snapshotCollectingProducer) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
	return nil
}
func (p *snapshotCollectingProducer) Close() error { return nil }

var _ engine_source.Producer = (*snapshotCollectingProducer)(nil)

type snapshotCrashingProducer struct {
	mu         sync.Mutex
	count      int
	crashAfter int
}

func (p *snapshotCrashingProducer) Connect(_ context.Context) error { return nil }
func (p *snapshotCrashingProducer) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	if p.count > p.crashAfter {
		return fmt.Errorf("simulated crash after %d events", p.crashAfter)
	}
	return nil
}
func (p *snapshotCrashingProducer) Close() error { return nil }

var _ engine_source.Producer = (*snapshotCrashingProducer)(nil)

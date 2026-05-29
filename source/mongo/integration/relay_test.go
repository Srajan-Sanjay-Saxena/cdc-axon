package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/core"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	mongoConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
	mongoSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/source"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcRabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func startMongo(t *testing.T, ctx context.Context) string {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:7",
		ExposedPorts: []string{"27017/tcp"},
		Cmd:          []string{"mongod", "--replSet", "rs0", "--bind_ip_all"},
		WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start mongo: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get mongo host: %v", err)
	}
	port, err := c.MappedPort(ctx, "27017")
	if err != nil {
		t.Fatalf("failed to get mongo port: %v", err)
	}
	uri := fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, port.Port())

	// initiate replica set
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("failed to connect to mongo: %v", err)
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: bson.D{}}}).Err(); err != nil {
		t.Fatalf("failed to initiate replica set: %v", err)
	}
	client.Disconnect(ctx)
	time.Sleep(3 * time.Second)

	return uri
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

func TestMongoSource_Connect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "outbox",
	}
	src := mongoSource.NewSource(cfg).AddProducer(&mockProducer{})

	if err := src.DBConnect(ctx); err != nil {
		t.Fatalf("DBConnect failed: %v", err)
	}
	defer src.Close(ctx)
}

func TestMongoSource_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)
	amqpURL := startRabbitMQ(t, ctx)

	prod := &rabbitProducer{url: amqpURL}
	if err := prod.Connect(ctx); err != nil {
		t.Fatalf("producer connect failed: %v", err)
	}
	defer prod.Close()

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "outbox",
	}
	src := mongoSource.NewSource(cfg).AddProducer(prod)
	engine := core.New(src)

	engineCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go engine.Start(engineCtx)
	time.Sleep(2 * time.Second)

	// insert into outbox
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(ctx)

	col := client.Database("testdb").Collection("outbox")
	_, err = col.InsertOne(ctx, bson.M{
		"_id":        "evt-1",
		"event_type": "ORDER_CREATED",
		"payload":    bson.M{"orderId": 1},
	})
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
		if got.ID != "evt-1" {
			t.Errorf("expected evt-1, got %s", got.ID)
		}
		if got.EventType != "ORDER_CREATED" {
			t.Errorf("expected ORDER_CREATED, got %s", got.EventType)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestMongoSource_EndToEnd_WithRedisPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	uri := startMongo(t, ctx)
	amqpURL := startRabbitMQ(t, ctx)
	rdb := startRedis(t, ctx)

	prod := &rabbitProducer{url: amqpURL}
	if err := prod.Connect(ctx); err != nil {
		t.Fatalf("producer connect failed: %v", err)
	}
	defer prod.Close()

	store := &redisStore{client: rdb}

	cfg := &mongoConfig.MongoRelaySourceConfig{
		URI:            uri,
		Database:       "testdb",
		CollectionName: "outbox",
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(ctx)
	col := client.Database("testdb").Collection("outbox")

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

	// --- First engine run ---
	crashCtx, crash := context.WithCancel(ctx)
	src1 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod)
	go core.New(src1).Start(crashCtx)
	time.Sleep(2 * time.Second)

	_, err = col.InsertOne(ctx, bson.M{
		"_id":        "evt-1",
		"event_type": "ORDER_CREATED",
		"payload":    bson.M{"orderId": 1},
	})
	if err != nil {
		t.Fatalf("insert evt-1 failed: %v", err)
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

	// verify resume token saved in Redis
	token, err := rdb.Get(ctx, "mongo:resume_token").Bytes()
	if err != nil || len(token) == 0 {
		t.Fatal("expected resume token in Redis after first run")
	}
	t.Logf("resume token saved in Redis: %d bytes", len(token))

	// simulate crash
	crash()
	time.Sleep(500 * time.Millisecond)

	// insert evt-2 while engine is down
	_, err = col.InsertOne(ctx, bson.M{
		"_id":        "evt-2",
		"event_type": "ORDER_UPDATED",
		"payload":    bson.M{"orderId": 2},
	})
	if err != nil {
		t.Fatalf("insert evt-2 failed: %v", err)
	}

	// --- Second engine run: resume from saved token ---
	src2 := mongoSource.NewSource(cfg).AddPersistanceStore(store).AddProducer(prod)
	engineCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()
	go core.New(src2).Start(engineCtx2)
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
		t.Logf("successfully received evt-2 after restart: %s", fmt.Sprintf("%+v", got))
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for evt-2 after restart")
	}
}

// mockProducer — in-memory producer
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

// rabbitProducer — real RabbitMQ producer
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

// redisStore — implements engine_source.PersistenceStore backed by Redis
type redisStore struct {
	client *redis.Client
}

func (r *redisStore) Connect(_ context.Context) error          { return nil }
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

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
	mongoConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
	mongoSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/source"
	pgConfig "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
	pgSource "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/source"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcRabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcRedis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func startPostgres(t *testing.T, ctx context.Context) string {
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

func startMongo(t *testing.T, ctx context.Context) string {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "mongo:7",
		ExposedPorts: []string{"27017/tcp"},
		Cmd:          []string{"mongod", "--replSet", "rs0", "--bind_ip_all"},
		WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start mongo: %v", err)
	}
	t.Cleanup(func() { c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "27017")
	uri := fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, port.Port())

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
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379")
	return redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
}

// TestEngine_PgAndMongo_Simultaneous runs both Postgres and Mongo engines
// concurrently against the same RabbitMQ queue and Redis store,
// verifying events from both sources are delivered correctly.
func TestEngine_PgAndMongo_Simultaneous(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	pgConnStr := startPostgres(t, ctx)
	mongoURI := startMongo(t, ctx)
	amqpURL := startRabbitMQ(t, ctx)
	rdb := startRedis(t, ctx)

	// setup postgres outbox
	pgxConn, err := pgx.Connect(ctx, pgConnStr)
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
		t.Fatalf("postgres setup failed: %v", err)
	}

	store := &redisStore{client: rdb}

	// pg producer
	pgProd := &rabbitProducer{url: amqpURL, queue: "pg_events"}

	// mongo producer
	mongoProd := &rabbitProducer{url: amqpURL, queue: "mongo_events"}

	// start pg engine
	pgSrc := pgSource.NewSource(&pgConfig.PgRelaySourceConfig{
		URL:             pgConnStr,
		SlotName:        "myslot",
		PublicationName: "mypub",
	}).AddPersistanceStore(store).AddProducer(pgProd)

	// start mongo engine
	mongoSrc := mongoSource.NewSource(&mongoConfig.MongoRelaySourceConfig{
		URI:            mongoURI,
		Database:       "testdb",
		CollectionName: "outbox",
	}).AddPersistanceStore(store).AddProducer(mongoProd)

	engineCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	go core.New(pgSrc).Start(engineCtx)
	go core.New(mongoSrc).Start(engineCtx)
	time.Sleep(3 * time.Second)

	// insert into postgres outbox
	pgxConn2, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		t.Fatalf("pgx connect failed: %v", err)
	}
	_, err = pgxConn2.Exec(ctx,
		`INSERT INTO outbox (id, event_type, operation, payload) VALUES ($1, $2, $3, $4)`,
		"pg-evt-1", "ORDER_CREATED", "insert", `{"source": "postgres"}`,
	)
	pgxConn2.Close(ctx)
	if err != nil {
		t.Fatalf("postgres insert failed: %v", err)
	}

	// insert into mongo outbox
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("mongo connect failed: %v", err)
	}
	defer mongoClient.Disconnect(ctx)
	col := mongoClient.Database("testdb").Collection("outbox")
	_, err = col.InsertOne(ctx, bson.M{
		"_id":        "mongo-evt-1",
		"event_type": "PAYMENT_PROCESSED",
		"payload":    bson.M{"source": "mongo"},
	})
	if err != nil {
		t.Fatalf("mongo insert failed: %v", err)
	}

	// consume from both queues concurrently
	amqpConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("amqp dial failed: %v", err)
	}
	defer amqpConn.Close()

	pgCh, _ := amqpConn.Channel()
	mongoCh, _ := amqpConn.Channel()
	defer pgCh.Close()
	defer mongoCh.Close()

	pgMsgs, _ := pgCh.Consume("pg_events", "", true, false, false, false, nil)
	mongoMsgs, _ := mongoCh.Consume("mongo_events", "", true, false, false, false, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		select {
		case msg := <-pgMsgs:
			var got event.Event
			json.Unmarshal(msg.Body, &got)
			if got.ID != "pg-evt-1" {
				t.Errorf("pg: expected pg-evt-1, got %s", got.ID)
			}
			if got.EventType != "ORDER_CREATED" {
				t.Errorf("pg: expected ORDER_CREATED, got %s", got.EventType)
			}
			t.Logf("pg event received: %+v", got)
		case <-time.After(30 * time.Second):
			t.Errorf("timed out waiting for pg event")
		}
	}()

	go func() {
		defer wg.Done()
		select {
		case msg := <-mongoMsgs:
			var got event.Event
			json.Unmarshal(msg.Body, &got)
			if got.ID != "mongo-evt-1" {
				t.Errorf("mongo: expected mongo-evt-1, got %s", got.ID)
			}
			if got.EventType != "PAYMENT_PROCESSED" {
				t.Errorf("mongo: expected PAYMENT_PROCESSED, got %s", got.EventType)
			}
			t.Logf("mongo event received: %+v", got)
		case <-time.After(30 * time.Second):
			t.Errorf("timed out waiting for mongo event")
		}
	}()

	wg.Wait()

	// verify Redis has both pg relation metadata and mongo resume token
	pgKeys, err := rdb.Keys(ctx, "relation:*").Result()
	if err != nil || len(pgKeys) == 0 {
		t.Error("expected pg relation metadata in Redis")
	}
	t.Logf("pg relation keys in Redis: %v", pgKeys)

	mongoToken, err := rdb.Get(ctx, "mongo:resume_token").Bytes()
	if err != nil || len(mongoToken) == 0 {
		t.Error("expected mongo resume token in Redis")
	}
	t.Logf("mongo resume token in Redis: %d bytes", len(mongoToken))
}

// rabbitProducer — real RabbitMQ producer with configurable queue
type rabbitProducer struct {
	url   string
	queue string
	conn  *amqp.Connection
	ch    *amqp.Channel
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
	_, err = ch.QueueDeclare(r.queue, true, false, false, false, nil)
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
	return r.ch.Publish("", r.queue, false, false, amqp.Publishing{
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

func (r *redisStore) Connect(_ context.Context) error              { return nil }
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

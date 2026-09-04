package source

import (
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/logger"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
	"go.mongodb.org/mongo-driver/mongo"
)

// The resume token is Mongo's equivalent of LSN. It's an opaque BSON token that points to a position in the oplog. On restart, you pass it back to Mongo and it resumes from exactly that point.

// key for redis to store the token
const resumeTokenKey = "mongo:resume_token"

/*
	MongoDB — Oplog + Change Streams
	Mongo has its own equivalent called the oplog (operations log). It's a capped collection that lives in the local database:

	db.local.oplog.rs

	Copy
	Every write to any collection gets appended here automatically (on replica sets). Just like WAL, it exists primarily for replication between primary and secondaries.

	INSERT into outbox
		↓
	Mongo writes to oplog (on replica sets, always)
		↓
	Change Stream tails the oplog
		↓
	Filters by your collection + operation type
		↓
	CDC-Axon reads decoded change events
		↓
	Ack → saves resume token to Redis
*/

type MongoRelaySource struct {
	cfg         *config.MongoRelaySourceConfig
	mongoClient *mongo.Client
	collection  *mongo.Collection
	store       engine_source.PersistenceStore
	producers   []engine_source.Producer
	cs          *mongo.ChangeStream
	log         *logger.Logger
}

func NewSource(cfg *config.MongoRelaySourceConfig) *MongoRelaySource {
	return &MongoRelaySource{
		cfg: cfg,
		log: logger.Default(),
	}
}

func (s *MongoRelaySource) SetLogger(l *logger.Logger) *MongoRelaySource {
	s.log = l
	return s
}

func (s *MongoRelaySource) AddPersistanceStore(store engine_source.PersistenceStore) *MongoRelaySource {
	s.store = store
	return s
}

func (s *MongoRelaySource) AddProducer(producer engine_source.Producer) *MongoRelaySource {
	s.producers = append(s.producers, producer)
	return s
}

func (s *MongoRelaySource) GetProducers() ([]engine_source.Producer, error) {
	if len(s.producers) == 0 {
		return nil, fmt.Errorf("no producers initialized")
	}
	return s.producers, nil
}

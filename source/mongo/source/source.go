package source

import (
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/config"
	"go.mongodb.org/mongo-driver/mongo"
)

const resumeTokenKey = "mongo:resume_token"

type MongoRelaySource struct {
	cfg        *config.MongoRelaySourceConfig
	mongoClient     *mongo.Client
	collection *mongo.Collection
	store      engine_source.PersistenceStore
	producer   engine_source.Producer
	cs         *mongo.ChangeStream
}

func NewSource(cfg *config.MongoRelaySourceConfig) *MongoRelaySource {
	return &MongoRelaySource{cfg: cfg}
}

func (s *MongoRelaySource) AddPersistanceStore(store engine_source.PersistenceStore) *MongoRelaySource {
	s.store = store
	return s
}

func (s *MongoRelaySource) AddProducer(producer engine_source.Producer) *MongoRelaySource {
	s.producer = producer
	return s
}

func (s *MongoRelaySource) GetProducer() (engine_source.Producer, error) {
	if s.producer == nil {
		return nil, fmt.Errorf("producer not initialized")
	}
	return s.producer, nil
}

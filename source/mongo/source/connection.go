package source

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (s *MongoRelaySource) DBConnect(ctx context.Context) error {
	s.log.Debug("mongo source: connecting", "uri", s.cfg.URI)

	if s.cfg.URI == "" {
		return fmt.Errorf("mongo source: missing URI in config")
	}

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(s.cfg.URI))
	if err != nil {
		return fmt.Errorf("mongo source: connect error: %w", err)
	}
	s.mongoClient = mongoClient
	s.collection = mongoClient.Database(s.cfg.Database).Collection(s.cfg.CollectionName)
	s.log.Info("mongo source: connected", "db", s.cfg.Database, "collection", s.cfg.CollectionName)

	return nil
}

func (s *MongoRelaySource) Close(ctx context.Context) error {
	if err := s.mongoClient.Disconnect(ctx); err != nil {
		s.log.Error("mongo source: disconnect error", "error", err)
		return err
	}
	return nil
}

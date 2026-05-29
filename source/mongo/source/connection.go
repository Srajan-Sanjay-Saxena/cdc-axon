package source

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func (s *MongoRelaySource) DBConnect(ctx context.Context) error {
	log.Println("Connecting to MongoDB...")

	if s.cfg.URI == "" {
		return fmt.Errorf("mongo source: missing URI in config")
	}

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(s.cfg.URI))
	if err != nil {
		return fmt.Errorf("mongo source: connect error: %w", err)
	}
	s.mongoClient = mongoClient
	s.collection = mongoClient.Database(s.cfg.Database).Collection(s.cfg.CollectionName)
	log.Println("Connected to MongoDB.....")

	return nil
}


func (s *MongoRelaySource) Close(ctx context.Context) error {
	if err := s.mongoClient.Disconnect(ctx); err != nil {
		log.Printf("Failed to disconnect from MongoDB: %v", err)
		return err
	}
	return nil
}

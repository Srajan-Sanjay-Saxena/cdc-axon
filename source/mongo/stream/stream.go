package stream

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ChangeEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  struct {
		ID        string `bson:"_id"`
		EventType string `bson:"event_type"`
		Payload   bson.M `bson:"payload"`
	} `bson:"fullDocument"`
}

func Open(ctx context.Context, collection *mongo.Collection, resumeToken bson.Raw) (*mongo.ChangeStream, error) {
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update"}}}}}}},
	}

	return collection.Watch(ctx, pipeline, opts)
}

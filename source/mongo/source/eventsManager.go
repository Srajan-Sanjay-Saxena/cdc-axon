package source

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/mongo/stream"
	"go.mongodb.org/mongo-driver/bson"
)
func (s *MongoRelaySource) CaptureEvents(ctx context.Context) (<-chan event.Event, error) {
	var resumeToken bson.Raw
	if s.store != nil {
		data, err := s.store.Load(ctx, resumeTokenKey)
		if err == nil && len(data) > 0 {
			resumeToken = bson.Raw(data)
		}
	}

	cs, err := stream.Open(ctx, s.collection, resumeToken)
	if err != nil {
		return nil, err
	}
	s.cs = cs

	ch := make(chan event.Event)

	go func() {
		defer close(ch)
		defer cs.Close(ctx)

		for cs.Next(ctx) {
			var raw stream.ChangeEvent
			if err := cs.Decode(&raw); err != nil {
				log.Printf("mongo source: decode error: %v", err)
				continue
			}

			payload, _ := json.Marshal(raw.FullDocument.Payload)

			ch <- event.Event{
				ID:        raw.FullDocument.ID,
				Source:    s.cfg.CollectionName,
				Operation: event.OperationType(raw.OperationType),
				EventType: raw.FullDocument.EventType,
				Payload:   payload,
			}
		}

		if err := cs.Err(); err != nil && ctx.Err() == nil {
			log.Printf("mongo source: change stream error: %v", err)
		}
	}()

	return ch, nil
}


func (s *MongoRelaySource) Ack(ctx context.Context) error {
	if s.store == nil || s.cs == nil {
		return nil
	}
	return s.store.Save(ctx, resumeTokenKey, []byte(s.cs.ResumeToken()))
}

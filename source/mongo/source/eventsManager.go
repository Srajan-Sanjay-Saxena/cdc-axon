package source

import (
	"context"
	"encoding/json"

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

	/*
		Open the change stream. If resumeToken is nil, Mongo opens it from now — meaning it will only see events that happen after this moment. Any events that occurred while CDC-Axon was down are gone.
	*/
	cs, err := stream.Open(ctx, s.collection, resumeToken)
	if err != nil {
		return nil, err
	}
	s.cs = cs

	ch := make(chan event.Event)

	go func() {
		defer close(ch)
		defer cs.Close(ctx)

		// blocking loop that waits for change events from Mongo and pushes them to the channel.
		for cs.Next(ctx) {
			var raw stream.ChangeEvent
			if err := cs.Decode(&raw); err != nil {
				s.log.Error("mongo source: decode error", "error", err)
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
			s.log.Error("mongo source: change stream error", "error", err)
		}
	}()

	return ch, nil
}

// will be used by our engine to save the resume token . In mongo there is no concept of SendStatus , but as we know the stream starts from the resume token that we save .
func (s *MongoRelaySource) Ack(ctx context.Context) error {
	if s.store == nil || s.cs == nil {
		return nil
	}
	return s.store.Save(ctx, resumeTokenKey, []byte(s.cs.ResumeToken()))
}

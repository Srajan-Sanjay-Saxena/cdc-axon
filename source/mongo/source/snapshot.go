package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/snapshot"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SyncSnapshot implements snapshot.SnapshotSource for MongoDB.
// It opens a Find cursor on the collection, reads documents in batches
// using keyset pagination on _id, and pushes each document as an event
// into the channel.
//
// Mode handling:
//   - Always + store: resumes from last saved cursor position. Resets after completion.
//   - Always + no store: starts from beginning every time.
//   - Initial + store (required): checks "snapshot:done" marker. If exists, skips.
//     Otherwise resumes from last cursor, saves "snapshot:done" after completion.
//   - Initial + no store: returns error.
func (s *MongoRelaySource) SyncSnapshot(ctx context.Context, table string, primaryKey string, batchSize int, mode snapshot.Mode, ch chan<- event.Event) error {
	// Initial mode requires a persistence store to track completion
	if mode == snapshot.Initial && s.store == nil {
		return fmt.Errorf("snapshot: Initial mode requires a PersistenceStore")
	}

	// Initial mode: check if snapshot was already completed previously
	if mode == snapshot.Initial && s.store != nil {
		data, _ := s.store.Load(ctx, "snapshot:done:"+table)
		if data != nil {
			s.log.Info("snapshot: already completed, skipping", "collection", table)
			return nil
		}
	}

	// connect to mongo if not already connected
	collection := s.mongoClient.Database(s.cfg.Database).Collection(table)

	// load last processed key from persistence store (resume point)
	var lastKey string
	if s.store != nil {
		data, _ := s.store.Load(ctx, "snapshot:cursor:"+table)
		if data != nil {
			lastKey = string(data)
			s.log.Info("snapshot: resuming from last position", "collection", table, "last_key", lastKey)
		}
	}

	totalRows := 0

	// loop: fetch batches using keyset pagination on _id
	for {
		// build filter based on whether we have a resume position
		filter := bson.D{}
		if lastKey != "" {
			filter = bson.D{{Key: primaryKey, Value: bson.D{{Key: "$gt", Value: lastKey}}}}
		}

		// sort by primary key and limit to batch size
		opts := options.Find().
			SetSort(bson.D{{Key: primaryKey, Value: 1}}).
			SetLimit(int64(batchSize))

		cursor, err := collection.Find(ctx, filter, opts)
		if err != nil {
			return fmt.Errorf("snapshot: find failed: %w", err)
		}

		count := 0
		var batchLastKey string

		// iterate over each document in this batch
		for cursor.Next(ctx) {
			// decode the raw document
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return fmt.Errorf("snapshot: decode failed: %w", err)
			}

			// extract primary key value
			id := fmt.Sprintf("%v", doc[primaryKey])
			batchLastKey = id

			// marshal entire document as JSON payload
			payloadBytes, err := json.Marshal(doc)
			if err != nil {
				cursor.Close(ctx)
				return fmt.Errorf("snapshot: marshal failed: %w", err)
			}

			ev := event.Event{
				ID:        id,
				Source:    table,
				Operation: event.INSERT,
				EventType: "SNAPSHOT",
				Payload:   payloadBytes,
			}

			// push event to channel — blocks until engine consumes it
			select {
			case ch <- ev:
			case <-ctx.Done():
				cursor.Close(ctx)
				return ctx.Err()
			}
			count++
		}

		cursor.Close(ctx)

		if cursor.Err() != nil {
			return fmt.Errorf("snapshot: cursor error: %w", cursor.Err())
		}

		// no more documents — done
		if count == 0 {
			break
		}

		// update lastKey for next iteration
		lastKey = batchLastKey

		// save cursor position after each batch so we can resume on crash
		if s.store != nil {
			s.store.Save(ctx, "snapshot:cursor:"+table, []byte(lastKey))
		}

		totalRows += count
		s.log.Debug("snapshot: batch processed", "rows_so_far", totalRows, "last_key", lastKey)
	}

	// snapshot complete — clean up cursor position and mark as done
	if s.store != nil {
		s.store.Delete(ctx, "snapshot:cursor:"+table)

		if mode == snapshot.Initial {
			s.store.Save(ctx, "snapshot:done:"+table, []byte("1"))
		}
	}

	return nil
}

// verify MongoRelaySource implements SnapshotSource at compile time
var _ snapshot.SnapshotSource = (*MongoRelaySource)(nil)

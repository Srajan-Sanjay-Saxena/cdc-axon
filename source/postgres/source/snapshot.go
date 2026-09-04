package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/snapshot"
	"github.com/jackc/pgx/v5"
)

// SyncSnapshot implements snapshot.SnapshotSource for Postgres.
// It opens a separate SQL connection (not replication), reads all rows
// from the given table using keyset pagination, and pushes each row
// as an event into the channel.
//
// Why snapshot exists:
// When you first deploy CDC-Axon, the outbox table might already have existing rows.
// WAL only captures NEW changes going forward. Snapshot backfills the historical data.
//
// Modes:
//
//   Initial = "Run snapshot ONCE ever, then never again."
//
//     Day 1: Deploy CDC-Axon
//         → Outbox has 100,000 existing rows
//         → Snapshot runs → reads all 100,000 → publishes to broker
//         → Saves "snapshot:done:outbox" in Redis
//         → WAL streaming starts
//
//     Day 2: CDC-Axon restarts
//         → Checks Redis: "snapshot:done:outbox" exists? YES
//         → Skip snapshot entirely
//         → WAL streaming starts immediately
//
//     Use case: Production. Backfill historical data once, then WAL handles everything.
//
//   Always = "Run snapshot EVERY single startup."
//
//     Day 1: Deploy CDC-Axon
//         → Snapshot runs → reads all rows → publishes
//         → Does NOT save "snapshot:done" marker
//         → WAL streaming starts
//
//     Day 2: CDC-Axon restarts
//         → Snapshot runs AGAIN → reads all rows again → publishes again
//         → WAL streaming starts
//
//     Use case: Testing, dev environment, or intentional full re-sync every time.
//
// Cursor (crash recovery, both modes):
// The cursor ("snapshot:cursor:outbox") is for crash recovery MID-snapshot.
//
//     Snapshot running, processed 50,000 of 100,000 rows
//         → Crash
//         → Cursor saved: "evt-50000"
//         → Restart
//         → Load cursor → resume from row 50,001
//         → Finish remaining 50,000
//         → Delete cursor
//
// Both Initial and Always use cursor for crash recovery. The difference is
// what happens AFTER snapshot completes:
//   - Initial: saves "done" marker → next startup skips snapshot
//   - Always: no "done" marker → next startup runs snapshot again
//
// Persistence requirements:
//   - Initial + no persistor: returns error (must track "done" state)
//   - Always + no persistor: works, but no crash recovery (restarts from beginning)
func (s *PgRelaySource) SyncSnapshot(ctx context.Context, table string, primaryKey string, batchSize int, mode snapshot.Mode, ch chan<- event.Event) error {
	// Initial mode requires a persistence store to track completion
	if mode == snapshot.Initial && s.walHandler.Persistor == nil {
		return fmt.Errorf("snapshot: Initial mode requires a PersistenceStore")
	}

	// Initial mode: check if snapshot was already completed previously
	if mode == snapshot.Initial && s.walHandler.Persistor != nil {
		data, _ := s.walHandler.Persistor.Load(ctx, "snapshot:done:"+table)
		if data != nil {
			s.log.Info("snapshot: already completed, skipping", "table", table)
			return nil
		}
	}

	// open a regular SQL connection (separate from the replication connection)
	// because replication protocol connections can't execute SELECT queries
	conn, err := pgx.Connect(ctx, s.cfg.URL)
	if err != nil {
		return fmt.Errorf("snapshot: connect failed: %w", err)
	}
	// close this connection when snapshot finishes (success or failure)
	defer conn.Close(ctx)

	// load last processed key from persistence store (resume point)
	// if no store or no saved key, start from the beginning
	var lastKey string
	if s.walHandler.Persistor != nil {
		data, _ := s.walHandler.Persistor.Load(ctx, "snapshot:cursor:"+table)
		if data != nil {
			lastKey = string(data)
			s.log.Info("snapshot: resuming from last position", "table", table, "last_key", lastKey)
		}
	}

	totalRows := 0

	// loop: fetch batches using keyset pagination until no more rows
	for {
		// build query based on whether we have a resume position
		var query string
		if lastKey == "" {
			// first batch — no resume point, start from beginning
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d", table, primaryKey, batchSize)
		} else {
			// resume from last processed key
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d", table, primaryKey, primaryKey, batchSize)
		}

		var rows pgx.Rows
		if lastKey == "" {
			rows, err = conn.Query(ctx, query)
		} else {
			rows, err = conn.Query(ctx, query, lastKey)
		}
		if err != nil {
			return fmt.Errorf("snapshot: query failed: %w", err)
		}

		count := 0
		var batchLastKey string

		// iterate over each row in this batch
		for rows.Next() {
			// convert the raw SQL row into an event.Event
			ev, err := rowToEvent(rows, table, primaryKey)
			if err != nil {
				rows.Close()
				return fmt.Errorf("snapshot: row conversion failed: %w", err)
			}

			// track the last key in this batch for cursor persistence
			batchLastKey = ev.ID

			// push event to channel — this blocks until the engine consumes it.
			// the engine runs in another goroutine, consuming from this channel,
			// running transforms, and publishing to producers.
			// if context is cancelled (shutdown), we stop immediately.
			select {
			case ch <- ev:
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			}
			count++
		}

		// release the rows object for this batch
		rows.Close()

		// check if iterating over rows produced an error
		if rows.Err() != nil {
			return fmt.Errorf("snapshot: rows error: %w", rows.Err())
		}

		// if this batch returned 0 rows, we've exhausted the table — done
		if count == 0 {
			break
		}

		// update lastKey for next iteration
		lastKey = batchLastKey

		// save cursor position after each batch so we can resume on crash
		if s.walHandler.Persistor != nil {
			s.walHandler.Persistor.Save(ctx, "snapshot:cursor:"+table, []byte(lastKey))
		}

		totalRows += count
		s.log.Debug("snapshot: batch processed", "rows_so_far", totalRows, "last_key", lastKey)
	}

	// snapshot complete — clean up cursor position and mark as done
	if s.walHandler.Persistor != nil {
		// remove cursor position (no longer needed)
		s.walHandler.Persistor.Delete(ctx, "snapshot:cursor:"+table)

		// Initial mode: mark as done so next startup skips snapshot
		if mode == snapshot.Initial {
			s.walHandler.Persistor.Save(ctx, "snapshot:done:"+table, []byte("1"))
		}
	}

	return nil
}

// rowToEvent converts a single SQL row into an event.Event.
// the entire row becomes the JSON payload. the primary key value becomes the event ID.
func rowToEvent(row pgx.Rows, table string, primaryKey string) (event.Event, error) {
	// Values() returns all column values for the current row as []interface{}
	values, err := row.Values()
	if err != nil {
		return event.Event{}, err
	}

	// FieldDescriptions() gives us column metadata (names, types)
	columns := row.FieldDescriptions()

	// build a map of column_name → value for the JSON payload
	payload := make(map[string]interface{}, len(columns))
	var id string

	for i, col := range columns {
		name := col.Name
		// every column goes into the payload
		payload[name] = values[i]
		// if this column is the primary key, extract its value as the event ID
		if name == primaryKey {
			id = fmt.Sprintf("%v", values[i])
		}
	}

	// marshal the entire row as JSON — this becomes the event payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return event.Event{}, err
	}

	// return the event:
	// - ID: primary key value (for dedup, routing, etc.)
	// - Source: table name
	// - Operation: always INSERT (snapshot = "inserting" existing state)
	// - EventType: "SNAPSHOT" marker so consumers know this is historical data
	// - Payload: entire row as JSON
	return event.Event{
		ID:        id,
		Source:    table,
		Operation: event.INSERT,
		EventType: "SNAPSHOT",
		Payload:   payloadBytes,
	}, nil
}

package source

import (
	"context"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/jackc/pgx/v5/pgconn"
)

// The main entry point for each driver in our cdc , which use to capture the events from wal/opolog from db . In pg case these are wal logs.
func (r *PgRelaySource) CaptureEvents(ctx context.Context) (<-chan event.Event, error) {
	if err := r.walHandler.SendStatus(ctx, r.pgConn); err != nil {
		return nil, err
	}

	ch := make(chan event.Event)

	go func() {
		defer close(ch)
		for {
			recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			msg, err := r.pgConn.ReceiveMessage(recvCtx)

			// Every context.WithTimeout allocates an internal timer. If ReceiveMessage returns before the 10-second timeout (which it will most of the time — WAL messages arrive in milliseconds), that timer is still running in the background until it expires. cancel stops that timer immmediately, preventing a memory leak. See https://golang.org/pkg/context/#WithTimeout for more details.
			
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					
					// Someone might ask that why even on not recieving the message we are sending the status to the server.

					/*
						WAL storage: Postgres writes WAL for everything — every INSERT, UPDATE, DELETE on every table in the entire database. This is how Postgres works internally for crash recovery, regardless of replication.

						WAL streaming: The replication slot + publication filters what gets sent to you.

						All tables in database
							↓
						WAL (stores everything — users, orders, payments, outbox, etc.)
							↓
						Replication slot (holds WAL back until consumer acks)
							↓
						pgoutput + Publication filter (only outbox table)
							↓
						CDC-Axon receives only outbox events

						What the Publication Does
						CREATE PUBLICATION mypub FOR TABLE outbox;
						This doesn't affect what Postgres stores in WAL. It affects what pgoutput decodes and sends to you.

						WAL contains:
						- INSERT into users (id=1)      ← stored, NOT sent to you
						- UPDATE orders SET status=...  ← stored, NOT sent to you
						- INSERT into outbox (evt-1)    ← stored, SENT to you
						- DELETE from logs WHERE...     ← stored, NOT sent to you

						So in this case if we donot send the status back to the server then wal data will pile up and will start choking the disk of pg server .
						But one question arrives that since we are not getting any message for long time (say) then how our lsn is moving forward , who is updating the clientLSN . The answer is that even if we are not getting any wal logs data , still pg sends the heartbeat message , and that's where we advances our clientLSN .
					*/

					r.walHandler.SendStatus(ctx, r.pgConn)
					continue
				}
				if ctx.Err() != nil {
					return
				}
				r.log.Error("postgres source: receive error", "error", err)
				return
			}

			result, err := r.walHandler.HandleMessage(ctx, msg)
			if err != nil {
				r.log.Error("postgres source: handle error", "error", err)
				return
			}

			if result.Event.ID != "" {
				ch <- result.Event
			}

			if result.Reply {
				r.walHandler.SendStatus(ctx, r.pgConn)
			}
		}
	}()

	return ch, nil
}

// Ack advances the confirmed LSN position after successful publish to all producers.
//
// Flow:
//   1. Event received from WAL → PendingLSN set to event's LSN
//   2. Event published to all producers successfully
//   3. Ack() called → ClientLSN = PendingLSN → SendStatus to Postgres
//   4. Postgres knows we've processed up to this LSN → can clean up WAL
//
// Why two LSNs (ClientLSN vs PendingLSN)?
//   - PendingLSN: "I received this event, haven't published yet"
//   - ClientLSN: "I successfully published, safe to advance"
//
// If publish fails:
//   - Ack() is NOT called
//   - ClientLSN stays at old position
//   - On restart, Postgres resends from ClientLSN → at-least-once delivery
//
// If publish succeeds but Ack() fails (connection dead):
//   - Engine detects error, retries
//   - On reconnect, Postgres resends the event (LSN wasn't advanced)
//   - Event gets published again → at-least-once (not exactly-once)
func (r *PgRelaySource) Ack(ctx context.Context) error {
	r.walHandler.ClientLSN = r.walHandler.PendingLSN
	return r.walHandler.SendStatus(ctx, r.pgConn)
}

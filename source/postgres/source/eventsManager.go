package source

import (
	"context"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/event"
	"github.com/jackc/pgx/v5/pgconn"
)

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

func (r *PgRelaySource) Ack(ctx context.Context) error {
	r.walHandler.ClientLSN = r.walHandler.PendingLSN
	return r.walHandler.SendStatus(ctx, r.pgConn)
}

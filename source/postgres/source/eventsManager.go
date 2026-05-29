package source

import (
	"context"
	"log"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/event"
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
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					r.walHandler.SendStatus(ctx, r.pgConn)
					continue
				}
				if ctx.Err() != nil {
					return
				}
				log.Printf("postgres source: receive error: %v", err)
				return
			}

			result, err := r.walHandler.HandleMessage(ctx, msg)
			if err != nil {
				log.Printf("postgres source: handle error: %v", err)
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

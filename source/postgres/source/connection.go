package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *PgRelaySource) DBConnect(ctx context.Context) error {
	cfg, err := pgconn.ParseConfig(r.cfg.URL)
	if err != nil {
		return err
	}

	// cfg.RuntimeParams["replication"]
	// Special parameter for the postgresql connection which tells to run the database in logical replication mode.
	// There are basically 3 options which we can give : --> 1. database 2. true 3. false
	// "false" (default) — normal query connection, no replication commands allowed
	// "database" — logical replication scoped to a specific database (what CDC-Axon needs)

	/*
		"true" puts the connection into physical replication mode. Here's what that means:

		What it does:

		Streams raw WAL bytes (binary blocks) directly from the primary server
		No decoding, no output plugin — just the raw write-ahead log segments
		Used by Postgres standby servers to stay in sync with the primary . Like for read-replicas and master synchornization.

		Primary (master)
    		↓  raw WAL binary blocks
		Standby (slave/read replica)
			→ applies raw blocks directly to its own data files
			→ ends up as an exact byte-for-byte copy of primary

		This is how Postgres read replicas work — AWS RDS read replicas, Aurora replicas, everything. They're all just standbys consuming raw physical WAL from the primary.	

	*/
	cfg.RuntimeParams["replication"] = "database"

	r.log.Debug("postgres source: connecting", "host", cfg.Host, "port", cfg.Port, "db", cfg.Database)

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}

	// storing the connection in the struct for later use
	r.pgConn = conn
	r.log.Info("postgres source: connected")

	if err := r.ensureSlot(ctx); err != nil {
		return err
	}
	return r.startReplication(ctx)
}

func (r *PgRelaySource) Close(ctx context.Context) error {
	if r.pgConn != nil {
		return r.pgConn.Close(ctx)
	}
	return nil
}



// Replication slot is a kind of bookmark maintained my postgres during the replication mode . It tracks how far a consumer has read in the WAL so Postgres knows it cannot discard WAL segments until the slot consumer has processed them.
func (r *PgRelaySource) ensureSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(
		ctx, r.pgConn, r.cfg.SlotName, config.OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Mode:      pglogrepl.LogicalReplication,
			// Temporary: false means the slot persists across Postgres restarts — this is CDC-Axon's crash recovery guarantee
			// When CDC-Axon restarts, it reconnects to the same slot and resumes from exactly where it left off (the saved LSN)
			Temporary: false,
		},
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			r.log.Debug("postgres source: slot already exists, continuing")
			return nil
		}
		return err
	}
	r.log.Info("postgres source: slot created", "slot", r.cfg.SlotName)
	return nil
}


// The error handling is specifically for the case where the replication slot is still active (i.e., another already died consumer is already using it). In that case, we retry a few times before giving up.
/*
	1. CDC-Axon is running normally
			↓
	2. CDC-Axon process dies (panic / container crash / OOM)
			↓
	3. The TCP connection between CDC-Axon and Postgres is now dead
	BUT Postgres doesn't know yet — TCP doesn't instantly notify the other side
			↓
	4. CDC-Axon restarts (new process)
			↓
	5. New process tries StartReplication on the same slot
			↓
	6. Postgres still thinks old TCP connection is alive (hasn't timed out yet)
	→ returns 55006 "slot is in use"
			↓
	7. CDC-Axon waits 3 seconds, retries
			↓
	8. Postgres finally detects old TCP is dead, releases the slot
			↓
	9. CDC-Axon connects successfully, resumes from last LSN

*/
func (r *PgRelaySource) startReplication(ctx context.Context) error {
	for i := 0; i < 10; i++ {
		err := pglogrepl.StartReplication(
			ctx, r.pgConn, r.cfg.SlotName, 0,
			pglogrepl.StartReplicationOptions{
				PluginArgs: []string{
					"proto_version '1'",
					fmt.Sprintf("publication_names '%s'", r.cfg.PublicationName),
				},
			},
		)
		if err == nil {
			r.log.Info("postgres source: replication started")
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55006" {
			r.log.Warn("postgres source: slot still active, retrying", "attempt", i+1);
			
			// Here i didn't go with exponential backoff because its the matter of few seconds and we want to retry as soon as possible. So, a fixed 3 seconds wait is good enough.
			time.Sleep(3 * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("slot %s still active after retries", r.cfg.SlotName)
}

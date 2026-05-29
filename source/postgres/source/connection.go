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
	cfg.RuntimeParams["replication"] = "database"

	r.log.Debug("postgres source: connecting", "host", cfg.Host, "port", cfg.Port, "db", cfg.Database)

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
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

func (r *PgRelaySource) ensureSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(
		ctx, r.pgConn, r.cfg.SlotName, config.OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Mode:      pglogrepl.LogicalReplication,
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
			r.log.Warn("postgres source: slot still active, retrying", "attempt", i+1)
			time.Sleep(3 * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("slot %s still active after retries", r.cfg.SlotName)
}

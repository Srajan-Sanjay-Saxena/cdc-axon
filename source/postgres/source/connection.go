package source

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/config"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *PgRelaySource) DBConnect(ctx context.Context) error {
	cfg, err := pgconn.ParseConfig(r.cfg.URL)
	if err != nil {
		return err
	}
	cfg.RuntimeParams["replication"] = "database"

	log.Printf("postgres source: connecting to host=%s port=%d db=%s", cfg.Host, cfg.Port, cfg.Database)

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	r.pgConn = conn
	log.Println("postgres source: connected")

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
			log.Println("postgres source: slot already exists, continuing")
			return nil
		}
		return err
	}
	log.Println("postgres source: slot created")
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
			log.Println("postgres source: replication started")
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55006" {
			log.Printf("postgres source: slot still active, retrying in 3s... (%d/10)", i+1)
			time.Sleep(3 * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("slot %s still active after retries", r.cfg.SlotName)
}

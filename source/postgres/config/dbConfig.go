package config

type PgRelaySourceConfig struct {
	// URL is the PostgreSQL connection string.
	URL string

	// SlotName is the replication slot name — a server-side bookmark that Postgres maintains.
	//
	// The slot tracks how far CDC-Axon has read in the WAL, so Postgres knows:
	//   - Where to resume streaming if CDC-Axon reconnects
	//   - Which WAL segments it cannot delete yet (consumer hasn't ack'd past them)
	//
	// Without a slot:
	//   Postgres WAL → gets cleaned up → CDC-Axon misses events
	//
	// With a slot:
	//   Postgres WAL → slot holds WAL back → CDC-Axon reads → acks → Postgres cleans up safely
	//
	// The slot survives Postgres restarts (Temporary: false in CreateReplicationSlot).
	// This is how crash recovery works — reconnect to the same slot, resume from last ack'd LSN.
	//
	// Example: SlotName = "cdc_axon_outbox_slot"
	SlotName string

	// PublicationName is the Postgres publication that filters which tables are streamed.
	//
	// Postgres writes WAL for ALL tables in the database. But CDC-Axon only cares about
	// the outbox table. The publication acts as a filter — pgoutput only decodes and sends
	// events for tables listed in the publication.
	//
	// Create it in Postgres:
	//   CREATE PUBLICATION mypub FOR TABLE outbox;
	//
	// What happens:
	//   INSERT into users  → WAL stored, NOT sent to CDC-Axon (not in publication)
	//   INSERT into orders → WAL stored, NOT sent to CDC-Axon (not in publication)
	//   INSERT into outbox → WAL stored, SENT to CDC-Axon (in publication)
	//
	// You can add multiple tables:
	//   CREATE PUBLICATION mypub FOR TABLE outbox, audit_log;
	//
	// Example: PublicationName = "mypub"
	PublicationName string
}

// OutputPlugin is PostgreSQL's built-in logical replication output plugin.
//
// When Postgres writes data, it first writes to WAL (Write-Ahead Log) as raw binary blocks.
// This binary format is internal to Postgres — page-level disk operations, unreadable by applications.
//
// pgoutput sits between the raw WAL and CDC-Axon:
//
//   WAL (raw binary) → pgoutput (decodes) → logical messages → pglogrepl (parses) → Go structs
//
// What pgoutput does:
//   - Decodes raw WAL binary into logical replication protocol messages
//   - Filters by Publication (only sends tables you subscribed to)
//   - Emits typed messages: RelationMessage, InsertMessage, UpdateMessage, DeleteMessage, etc.
//
// What pglogrepl does:
//   - Parses pgoutput's wire format into Go structs (pglogrepl.InsertMessage, etc.)
//   - CDC-Axon then extracts id, event_type, payload from these structs
//
// Why pgoutput over alternatives (wal2json, etc.):
//   - Built into Postgres (no extension needed, works on RDS/Aurora/Cloud SQL)
//   - pglogrepl is designed to parse pgoutput's format
//   - Binary protocol = more efficient than JSON-based plugins
const (
	OutputPlugin = "pgoutput"
)
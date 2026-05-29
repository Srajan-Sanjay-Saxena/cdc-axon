package config

type PgRelaySourceConfig struct {
	URL     string 
	SlotName string
	PublicationName string
}

const (
	OutputPlugin = "pgoutput"
)
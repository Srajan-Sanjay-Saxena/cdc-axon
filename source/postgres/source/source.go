package source

import (
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/config"
	walhandler "github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/walHandlers"
	"github.com/jackc/pgx/v5/pgconn"
)

type PgRelaySource struct {
	cfg		*config.PgRelaySourceConfig
	pgConn       *pgconn.PgConn
	walHandler *walhandler.WalHandlers
	producer  engine_source.Producer
}

func NewSource(cfg *config.PgRelaySourceConfig) *PgRelaySource {
	return &PgRelaySource{
		walHandler: walhandler.NewWalHandlers(),
		cfg: cfg,
	}
}

func (s *PgRelaySource) AddPersistanceStore(store engine_source.PersistenceStore) *PgRelaySource {
	s.walHandler.Persistor = store
	return s
}

func (s *PgRelaySource) AddProducer(producer engine_source.Producer) *PgRelaySource {
	s.producer = producer
	return s
}

func (s *PgRelaySource) GetProducer() (engine_source.Producer, error) {
	if s.producer == nil {
		return nil, fmt.Errorf("producer not initialized")
	}
	return s.producer, nil
}

package source

import (
	"fmt"

	"github.com/Srajan-Sanjay-Saxena/cdc-axon/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/config"
	walhandler "github.com/Srajan-Sanjay-Saxena/cdc-axon/source/postgres/walHandlers"
	"github.com/jackc/pgx/v5/pgconn"
)

type PgRelaySource struct {
	cfg        *config.PgRelaySourceConfig
	pgConn     *pgconn.PgConn
	walHandler *walhandler.WalHandlers
	producers  []engine_source.Producer
}

func NewSource(cfg *config.PgRelaySourceConfig) *PgRelaySource {
	return &PgRelaySource{
		walHandler: walhandler.NewWalHandlers(),
		cfg:        cfg,
	}
}

func (s *PgRelaySource) AddPersistanceStore(store engine_source.PersistenceStore) *PgRelaySource {
	s.walHandler.Persistor = store
	return s
}

func (s *PgRelaySource) AddProducer(producer engine_source.Producer) *PgRelaySource {
	s.producers = append(s.producers, producer)
	return s
}

func (s *PgRelaySource) GetProducers() ([]engine_source.Producer, error) {
	if len(s.producers) == 0 {
		return nil, fmt.Errorf("no producers initialized")
	}
	return s.producers, nil
}

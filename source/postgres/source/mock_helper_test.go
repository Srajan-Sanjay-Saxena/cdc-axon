package source

import (
	"context"
	"errors"

	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/engine_source"
	"github.com/Srajan-Sanjay-Saxena/cdcrelay/engine/event"
	walhandler "github.com/Srajan-Sanjay-Saxena/cdcrelay/source/postgres/walHandlers"
)

type MockProducer struct {
	events     []event.Event
	shouldFail bool
}

func (m *MockProducer) Connect(_ context.Context) error { return nil }

func (m *MockProducer) Publish(_ context.Context, e event.Event) error {
	if m.shouldFail {
		return errors.New("publish failed")
	}
	m.events = append(m.events, e)
	return nil
}

func (m *MockProducer) Close() error { return nil }

var _ engine_source.Producer = (*MockProducer)(nil)

func newTestWalHandler() *walhandler.WalHandlers {
	return walhandler.NewWalHandlers()
}

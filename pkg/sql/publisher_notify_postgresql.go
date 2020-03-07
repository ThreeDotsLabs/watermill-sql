package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type NotifyPostgresPublisherConfig struct {
	Marshaler PostgresNotifyMarshaler
}

func (c NotifyPostgresPublisherConfig) validate() error {
	if c.Marshaler == nil {
		return errors.New("marshaler is nil")
	}
	return nil
}

func (c *NotifyPostgresPublisherConfig) setDefaults() {}

// NotifyPostgresPublisher inserts the Messages as rows into a SQL table..
type NotifyPostgresPublisher struct {
	config NotifyPostgresPublisherConfig

	db contextExecutor

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
}

func NewNotifyPostgresPublisher(db contextExecutor, config NotifyPostgresPublisherConfig, logger watermill.LoggerAdapter) (*NotifyPostgresPublisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &NotifyPostgresPublisher{
		config: config,
		db:     db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,

		logger: logger,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
// Order is guaranteed for messages within one call.
// Publish is blocking until all rows have been added to the NotifyPostgresPublisher's transaction.
// NotifyPostgresPublisher doesn't guarantee publishing messages in a single transaction,
// but the constructor accepts both *sql.DB and *sql.Tx, so transactions may be handled upstream by the user.
func (p *NotifyPostgresPublisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	if err := validateTopicName(topic); err != nil {
		return err
	}

	for _, msg := range messages {
		err = p.publishMessage(topic, msg)
		if err != nil {
			return errors.Wrap(err, "error while publishing message via NOTIFY")
		}
	}

	return nil
}

func (p *NotifyPostgresPublisher) publishMessage(topic string, msg *message.Message) error {
	p.logger.Trace("Publishing message with NOTIFY", watermill.LogFields{
		"topic": topic,
		"uuid":  msg.UUID,
	})

	payload, err := p.config.Marshaler.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "cannot marshal message")
	}

	_, err = p.db.ExecContext(context.Background(), fmt.Sprintf("NOTIFY %s, '%s'", topic, payload))
	if err != nil {
		return errors.Wrap(err, "could not NOTIFY")
	}

	return nil
}

// Close closes the publisher, which means that all the Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *NotifyPostgresPublisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}

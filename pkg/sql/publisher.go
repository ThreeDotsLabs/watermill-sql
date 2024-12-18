package sql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	ErrPublisherClosed = errors.New("publisher is closed")
)

type PublisherConfig struct {
	// SchemaAdapter provides the schema-dependent queries and arguments for them, based on topic/message etc.
	SchemaAdapter SchemaAdapter

	// AutoInitializeSchema enables initialization of schema database during publish.
	// Schema is initialized once per topic per publisher instance.
	// AutoInitializeSchema is forbidden if using an ongoing transaction as database handle;
	// That could result in an implicit commit of the transaction by a CREATE TABLE statement.
	AutoInitializeSchema bool
}

func (c PublisherConfig) validate() error {
	if c.SchemaAdapter == nil {
		return errors.New("schema adapter is nil")
	}

	return nil
}

func (c *PublisherConfig) setDefaults() {
}

// Publisher inserts the Messages as rows into a SQL table..
type Publisher struct {
	config PublisherConfig

	db ContextExecutor

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
}

func NewPublisher(db ContextExecutor, config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.AutoInitializeSchema && isTx(db) {
		// either use a prior schema with a tx db handle, or don't use tx with AutoInitializeSchema
		return nil, errors.New("tried to use AutoInitializeSchema with a database handle that looks like" +
			"an ongoing transaction; this may result in an implicit commit")
	}

	return &Publisher{
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
// Publish is blocking until all rows have been added to the Publisher's transaction.
// Publisher doesn't guarantee publishing messages in a single transaction,
// but the constructor accepts both *sql.DB and *sql.Tx, so transactions may be handled upstream by the user.
func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	if err := validateTopicName(topic); err != nil {
		return err
	}

	if err := p.initializeSchema(topic); err != nil {
		return err
	}

	insertQuery, err := p.config.SchemaAdapter.InsertQuery(InsertQueryParams{
		Topic: topic,
		Msgs:  messages,
	})
	if err != nil {
		return fmt.Errorf("cannot create insert query: %w", err)
	}

	p.logger.Trace("Inserting message to SQL", watermill.LogFields{
		"query":      insertQuery.Query,
		"query_args": sqlArgsToLog(insertQuery.Args),
	})

	_, err = p.db.ExecContext(context.Background(), insertQuery.Query, insertQuery.Args...)
	if err != nil {
		return fmt.Errorf("could not insert message as row: %w", err)
	}

	return nil
}

func (p *Publisher) initializeSchema(topic string) error {
	if !p.config.AutoInitializeSchema {
		return nil
	}

	if _, ok := p.initializedTopics.Load(topic); ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	if err := initializeSchema(
		ctx,
		topic,
		p.logger,
		p.db,
		p.config.SchemaAdapter,
		nil,
	); err != nil {
		return fmt.Errorf("cannot initialize schema: %w", err)
	}

	p.initializedTopics.Store(topic, struct{}{})
	return nil
}

// Close closes the publisher, which means that all the Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}

func isTx(db ContextExecutor) bool {
	_, dbIsTx := db.(interface {
		Commit() error
		Rollback() error
	})
	return dbIsTx
}

package sql

import (
	"database/sql"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayedPostgreSQLPublisherConfig struct {
	DelayPublisherConfig    delay.PublisherConfig
	OverridePublisherConfig func(config *PublisherConfig) error
	Logger                  watermill.LoggerAdapter
}

func (c *DelayedPostgreSQLPublisherConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedPostgreSQLPublisher creates a new Publisher that stores messages in PostgreSQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedPostgreSQLPublisher(db *sql.DB, config DelayedPostgreSQLPublisherConfig) (message.Publisher, error) {
	config.setDefaults()

	publisherConfig := PublisherConfig{
		SchemaAdapter:        ConditionalPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}

	if config.OverridePublisherConfig != nil {
		err := config.OverridePublisherConfig(&publisherConfig)
		if err != nil {
			return nil, err
		}
	}

	var publisher message.Publisher
	var err error

	publisher, err = NewPublisher(db, publisherConfig, config.Logger)
	if err != nil {
		return nil, err
	}

	publisher, err = delay.NewPublisher(publisher, config.DelayPublisherConfig)
	if err != nil {
		return nil, err
	}

	return publisher, nil
}

type DelayedPostgreSQLSubscriberConfig struct {
	OverrideSubscriberConfig func(config *SubscriberConfig) error
	DeleteOnAck              bool
	Logger                   watermill.LoggerAdapter
}

func (c *DelayedPostgreSQLSubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedPostgreSQLSubscriber creates a new Subscriber that reads messages from PostgreSQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedPostgreSQLSubscriber(db *sql.DB, config DelayedPostgreSQLSubscriberConfig) (message.Subscriber, error) {
	schemaAdapter := delayedPostgreSQLSchemaAdapter{
		ConditionalPostgreSQLSchema: ConditionalPostgreSQLSchema{
			GenerateWhereClause: func(params GenerateWhereClauseParams) (string, []any) {
				return fmt.Sprintf("(metadata->>'%v')::timestamptz < NOW() AT TIME ZONE 'UTC'", delay.DelayedUntilKey), nil
			},
		},
	}

	subscriberConfig := SubscriberConfig{
		SchemaAdapter: schemaAdapter,
		OffsetsAdapter: ConditionalPostgreSQLOffsetsAdapter{
			DeleteOnAck: config.DeleteOnAck,
		},
		InitializeSchema: true,
	}

	if config.OverrideSubscriberConfig != nil {
		err := config.OverrideSubscriberConfig(&subscriberConfig)
		if err != nil {
			return nil, err
		}
	}

	sub, err := NewSubscriber(db, subscriberConfig, config.Logger)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

type delayedPostgreSQLSchemaAdapter struct {
	ConditionalPostgreSQLSchema
}

func (a delayedPostgreSQLSchemaAdapter) SchemaInitializingQueries(params SchemaInitializingQueriesParams) []Query {
	queries := a.ConditionalPostgreSQLSchema.SchemaInitializingQueries(params)

	queries = append(queries, Query{
		Query: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_delayed_until_idx ON %s (metadata->>'%s')`, params.Topic, params.Topic, delay.DelayedUntilKey),
	})

	return queries
}

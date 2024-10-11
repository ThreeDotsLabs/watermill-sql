package sql

import (
	"database/sql"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayedPostgresPublisherConfig struct {
	DelayPublisherConfig    delay.PublisherConfig
	OverridePublisherConfig func(config *PublisherConfig) error
	Logger                  watermill.LoggerAdapter
}

func (c *DelayedPostgresPublisherConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedPostgresPublisher creates a new Publisher that stores messages in PostgreSQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedPostgresPublisher(db *sql.DB, config DelayedPostgresPublisherConfig) (message.Publisher, error) {
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

type DelayedPostgresSubscriberConfig struct {
	OverrideSubscriberConfig func(config *SubscriberConfig) error
	DeleteOnAck              bool
	Logger                   watermill.LoggerAdapter
}

func (c *DelayedPostgresSubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedPostgresSubscriber creates a new Subscriber that reads messages from PostgreSQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedPostgresSubscriber(db *sql.DB, config DelayedPostgresSubscriberConfig) (message.Subscriber, error) {
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

func (a delayedPostgreSQLSchemaAdapter) SchemaInitializingQueries(topic string) []Query {
	queries := a.ConditionalPostgreSQLSchema.SchemaInitializingQueries(topic)

	queries = append(queries, Query{
		Query: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_delayed_until_idx ON %s (metadata->>'%s')`, topic, topic, delay.DelayedUntilKey),
	})

	return queries
}

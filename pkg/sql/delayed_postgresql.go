package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayedPostgreSQLPublisherConfig struct {
	// DelayPublisherConfig is a configuration for the delay.Publisher.
	DelayPublisherConfig delay.PublisherConfig

	// OverridePublisherConfig allows overriding the default PublisherConfig.
	OverridePublisherConfig func(config *PublisherConfig) error

	Logger watermill.LoggerAdapter
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
		SchemaAdapter:        PostgreSQLQueueSchema{},
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
	config.setDefaults()

	schemaAdapter := delayedPostgreSQLSchemaAdapter{
		PostgreSQLQueueSchema: PostgreSQLQueueSchema{
			GenerateWhereClause: func(params GenerateWhereClauseParams) (string, []any) {
				return fmt.Sprintf("(metadata->>'%v')::timestamptz < NOW() AT TIME ZONE 'UTC'", delay.DelayedUntilKey), nil
			},
		},
	}

	subscriberConfig := SubscriberConfig{
		SchemaAdapter: schemaAdapter,
		OffsetsAdapter: PostgreSQLQueueOffsetsAdapter{
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
	PostgreSQLQueueSchema
}

func (a delayedPostgreSQLSchemaAdapter) SchemaInitializingQueries(params SchemaInitializingQueriesParams) ([]Query, error) {
	queries, err := a.PostgreSQLQueueSchema.SchemaInitializingQueries(params)
	if err != nil {
		return nil, err
	}

	table := a.MessagesTable(params.Topic)
	index := fmt.Sprintf(`"%s_delayed_until_idx"`, strings.ReplaceAll(table, `"`, ""))

	queries = append(queries, Query{
		Query: fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s ((metadata->>'%s'))`, index, table, delay.DelayedUntilKey),
	})

	return queries, nil
}

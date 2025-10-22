package sql

import (
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayedMySQLPublisherConfig struct {
	// DelayPublisherConfig is a configuration for the delay.Publisher.
	DelayPublisherConfig delay.PublisherConfig

	// OverridePublisherConfig allows overriding the default PublisherConfig.
	OverridePublisherConfig func(config *PublisherConfig) error

	Logger watermill.LoggerAdapter
}

func (c *DelayedMySQLPublisherConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedMySQLPublisher creates a new Publisher that stores messages in MySQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedMySQLPublisher(db ContextExecutor, config DelayedMySQLPublisherConfig) (message.Publisher, error) {
	config.setDefaults()

	publisherConfig := PublisherConfig{
		SchemaAdapter:        MySQLQueueSchema{},
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

type DelayedMySQLSubscriberConfig struct {
	// OverrideSubscriberConfig allows overriding the default SubscriberConfig.
	OverrideSubscriberConfig func(config *SubscriberConfig) error

	// DeleteOnAck deletes the message from the queue when it's acknowledged.
	DeleteOnAck bool

	// AllowNoDelay allows receiving messages without the delay metadata.
	// By default, such messages will be skipped.
	// If set to true, messages without delay metadata will be received immediately.
	AllowNoDelay bool

	Logger watermill.LoggerAdapter
}

func (c *DelayedMySQLSubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

// NewDelayedMySQLSubscriber creates a new Subscriber that reads messages from MySQL with a delay.
// The delay can be set per message with the Watermill's components/delay metadata.
func NewDelayedMySQLSubscriber(db Beginner, config DelayedMySQLSubscriberConfig) (message.Subscriber, error) {
	config.setDefaults()

	where := fmt.Sprintf("STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.%s')), '%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ') < UTC_TIMESTAMP()", delay.DelayedUntilKey)

	if config.AllowNoDelay {
		where += fmt.Sprintf(` OR JSON_EXTRACT(metadata, '$.%s') IS NULL`, delay.DelayedUntilKey)
	}

	schemaAdapter := delayedMySQLSchemaAdapter{
		MySQLQueueSchema: MySQLQueueSchema{
			GenerateWhereClause: func(params GenerateWhereClauseParams) (string, []any) {
				return where, nil
			},
		},
	}

	subscriberConfig := SubscriberConfig{
		SchemaAdapter: schemaAdapter,
		OffsetsAdapter: MySQLQueueOffsetsAdapter{
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

type delayedMySQLSchemaAdapter struct {
	MySQLQueueSchema
}

func (a delayedMySQLSchemaAdapter) SchemaInitializingQueries(params SchemaInitializingQueriesParams) ([]Query, error) {
	queries, err := a.MySQLQueueSchema.SchemaInitializingQueries(params)
	if err != nil {
		return nil, err
	}

	table := a.MessagesTable(params.Topic)
	index := fmt.Sprintf("`%s_delayed_until_idx`", strings.ReplaceAll(strings.Trim(table, "`"), "`", ""))

	queries = append(queries, Query{
		Query: fmt.Sprintf(`CREATE INDEX %s ON %s ((JSON_EXTRACT(metadata, '$.%s')))`, index, table, delay.DelayedUntilKey),
	})

	return queries, nil
}

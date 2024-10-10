package sql

import (
	"database/sql"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayedPostgresPublisherConfig struct {
	DelayPublisherConfig delay.PublisherConfig
	Logger               watermill.LoggerAdapter
}

func (c *DelayedPostgresPublisherConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func NewDelayedPostgresPublisher(db *sql.DB, config DelayedPostgresPublisherConfig) (message.Publisher, error) {
	config.setDefaults()

	var publisher message.Publisher
	var err error

	publisher, err = NewPublisher(db, PublisherConfig{
		SchemaAdapter: ConditionalPostgreSQLSchema{},
	}, config.Logger)
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
	Logger watermill.LoggerAdapter
}

func (c *DelayedPostgresSubscriberConfig) setDefaults() {
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

func NewDelayedPostgresSubscriber(db *sql.DB, config DelayedPostgresSubscriberConfig) (message.Subscriber, error) {
	sub, err := NewSubscriber(db, SubscriberConfig{
		SchemaAdapter: ConditionalPostgreSQLSchema{
			GenerateWhereClause: func(params GenerateWhereClauseParams) (string, []any) {
				return fmt.Sprintf("(metadata->>'%v')::timestamptz < NOW() AT TIME ZONE 'UTC'", delay.DelayedUntilKey), nil
			},
		},
		// TODO configurable?
		OffsetsAdapter: ConditionalPostgreSQLOffsetsAdapter{
			DeleteOnAck: true,
		},
		// TODO configurable?
		InitializeSchema: true,
	}, config.Logger)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

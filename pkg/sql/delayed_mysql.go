package sql

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
		SchemaAdapter: delayedMySQLSchemaAdapter{
			MySQLQueueSchema: MySQLQueueSchema{},
		},
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

	where := "delayed_until <= UTC_TIMESTAMP()"

	if config.AllowNoDelay {
		where += " OR delayed_until IS NULL"
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
	createMessagesTable := `
		CREATE TABLE IF NOT EXISTS ` + a.MessagesTable(params.Topic) + ` (
 			` + "`offset`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
 			` + "`uuid`" + ` VARCHAR(36) NOT NULL,
 			` + "`payload`" + ` ` + a.payloadColumnType(params.Topic) + ` DEFAULT NULL,
 			` + "`metadata`" + ` JSON DEFAULT NULL,
 			` + "`acked`" + ` BOOLEAN NOT NULL DEFAULT FALSE,
 			` + "`created_at`" + ` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 			` + "`delayed_until`" + ` TIMESTAMP NULL DEFAULT NULL,
 			INDEX ` + "`idx_acked_delayed`" + ` (` + "`acked`" + `, ` + "`delayed_until`" + `)
 		);
	`

	return []Query{{Query: createMessagesTable}}, nil
}

func (a delayedMySQLSchemaAdapter) InsertQuery(params InsertQueryParams) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata, delayed_until) VALUES %s`,
		a.MessagesTable(params.Topic),
		delayedMySQLInsertMarkers(len(params.Msgs)),
	)

	args, err := delayedMySQLInsertArgs(params.Msgs)
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func delayedMySQLInsertMarkers(count int) string {
	result := strings.Builder{}

	for range count {
		result.WriteString("(?,?,?,?),")
	}

	return strings.TrimRight(result.String(), ",")
}

func delayedMySQLInsertArgs(msgs message.Messages) ([]any, error) {
	var args []any

	for _, msg := range msgs {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return nil, fmt.Errorf("could not marshal metadata into JSON for message %s: %w", msg.UUID, err)
		}

		args = append(args, msg.UUID, msg.Payload, metadata)

		// Extract delayed_until from metadata
		delayedUntilStr := msg.Metadata.Get(delay.DelayedUntilKey)
		if delayedUntilStr == "" {
			args = append(args, nil)
		} else {
			delayedUntil, err := time.Parse(time.RFC3339, delayedUntilStr)
			if err != nil {
				return nil, fmt.Errorf("could not parse delayed_until timestamp %s: %w", delayedUntilStr, err)
			}
			args = append(args, delayedUntil)
		}
	}

	return args, nil
}

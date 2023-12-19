package sql

import (
	"fmt"
)

// DefaultMySQLOffsetsAdapter is adapter for storing offsets for MySQL (or MariaDB) databases.
//
// DefaultMySQLOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We are using FOR UPDATE in NextOffsetQuery to lock consumer group in offsets table.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultMySQLOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultMySQLOffsetsAdapter) SchemaInitializingQueries(topic string) []Query {
	return []Query{
		{
			Query: `
				CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
				consumer_group VARCHAR(255) NOT NULL,
				offset_acked BIGINT,
				offset_consumed BIGINT NOT NULL,
				PRIMARY KEY(consumer_group)
			)`,
		},
	}
}

func (a DefaultMySQLOffsetsAdapter) AckMessageQuery(topic string, row Row, consumerGroup string) Query {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, offset_acked, consumer_group)
		VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed), offset_acked=VALUES(offset_acked)`

	return Query{ackQuery, []any{row.Offset, row.Offset, consumerGroup}}
}

func (a DefaultMySQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) Query {
	return Query{
		Query: `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=? FOR UPDATE
				), 0)`,
		Args: []any{consumerGroup},
	}
}

func (a DefaultMySQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("`watermill_offsets_%s`", topic)
}

func (a DefaultMySQLOffsetsAdapter) ConsumedMessageQuery(topic string, row Row, consumerGroup string, consumerULID []byte) Query {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed)`
	return Query{ackQuery, []interface{}{row.Offset, consumerGroup}}
}

func (a DefaultMySQLOffsetsAdapter) BeforeSubscribingQueries(topic, consumerGroup string) []Query {
	return nil
}

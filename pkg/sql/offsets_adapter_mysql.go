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

func (a DefaultMySQLOffsetsAdapter) SchemaInitializingQueries(params OffsetsSchemaInitializingQueriesParams) ([]Query, error) {
	return []Query{
		{
			Query: `
				CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(params.Topic) + ` (
				consumer_group VARCHAR(255) NOT NULL,
				offset_acked BIGINT,
				offset_consumed BIGINT NOT NULL,
				PRIMARY KEY(consumer_group)
			)`,
		},
	}, nil
}

func (a DefaultMySQLOffsetsAdapter) AckMessageQuery(params AckMessageQueryParams) (Query, error) {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(params.Topic) + ` (offset_consumed, offset_acked, consumer_group)
		VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed), offset_acked=VALUES(offset_acked)`

	return Query{ackQuery, []any{params.LastRow.Offset, params.LastRow.Offset, params.ConsumerGroup}}, nil
}

func (a DefaultMySQLOffsetsAdapter) NextOffsetQuery(params NextOffsetQueryParams) (Query, error) {
	return Query{
		Query: `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(params.Topic) + `
				 WHERE consumer_group=? FOR UPDATE
				), 0)`,
		Args: []any{params.ConsumerGroup},
	}, nil
}

func (a DefaultMySQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("`watermill_offsets_%s`", topic)
}

func (a DefaultMySQLOffsetsAdapter) ConsumedMessageQuery(params ConsumedMessageQueryParams) (Query, error) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(params.Topic) + ` (offset_consumed, consumer_group)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed)`
	return Query{ackQuery, []interface{}{params.Row.Offset, params.ConsumerGroup}}, nil
}

func (a DefaultMySQLOffsetsAdapter) BeforeSubscribingQueries(params BeforeSubscribingQueriesParams) ([]Query, error) {
	return nil, nil
}

package sql

import (
	"fmt"
)

// DefaultPostgreSQLOffsetsAdapter is adapter for storing offsets in PostgreSQL database.
//
// DefaultPostgreSQLOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We are using FOR UPDATE in NextOffsetQuery to lock consumer group in offsets table.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultPostgreSQLOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultPostgreSQLOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`
		CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset_acked BIGINT,
		offset_consumed BIGINT NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (a DefaultPostgreSQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=$1 FOR UPDATE
				), 0)`,
		[]interface{}{consumerGroup}
}

func (a DefaultPostgreSQLOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `UPDATE ` + a.MessagesOffsetsTable(topic) + ` SET offset_acked = $1 WHERE consumer_group = $2`
	return ackQuery, []interface{}{offset, consumerGroup}
}

func (a DefaultPostgreSQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf(`"watermill_offsets_%s"`, topic)
}

func (a DefaultPostgreSQLOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES ($1, $2) ON CONFLICT("consumer_group") DO UPDATE SET offset_consumed=excluded.offset_consumed`
	return ackQuery, []interface{}{offset, consumerGroup}
}

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

func (a DefaultMySQLOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`
		CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset_acked BIGINT,
		offset_consumed BIGINT NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (a DefaultMySQLOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `UPDATE ` + a.MessagesOffsetsTable(topic) + ` SET offset_acked = ? WHERE consumer_group = ?`
	return ackQuery, []interface{}{offset, consumerGroup}
}

func (a DefaultMySQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=? FOR UPDATE
				), 0)`,
		[]interface{}{consumerGroup}
}

func (a DefaultMySQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("`watermill_offsets_%s`", topic)
}

func (a DefaultMySQLOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed)`
	return ackQuery, []interface{}{offset, consumerGroup}
}

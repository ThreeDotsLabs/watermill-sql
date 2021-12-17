package sql

import "fmt"

// DefaultMySQLBinlogOffsetsAdapter is adapter for storing offsets for MySQL (or MariaDB) databases.
//
// DefaultMySQLBinlogOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We are using FOR UPDATE in NextOffsetQuery to lock consumer group in offsets table.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultMySQLBinlogOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultMySQLBinlogOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset_acked BIGINT,
		offset_consumed BIGINT NOT NULL,
		log_name VARCHAR(255) NOT NULL,
		log_position INT UNSIGNED NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (a DefaultMySQLBinlogOffsetsAdapter) ConsumedMessageQuery(topic string, offset int, consumerGroup string, logName string, logPosition uint32) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group, log_name, log_position)
		VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed),log_name=VALUES(log_name), log_position=VALUES(log_position)`
	return ackQuery, []interface{}{offset, consumerGroup, logName, logPosition}
}

func (a DefaultMySQLBinlogOffsetsAdapter) PositionQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT log_name, log_position
			FROM ` + a.MessagesOffsetsTable(topic) + `
			WHERE consumer_group=?`,
		[]interface{}{consumerGroup}
}

func (a DefaultMySQLBinlogOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `UPDATE ` + a.MessagesOffsetsTable(topic) + ` SET offset_acked = ? WHERE consumer_group = ?`
	return ackQuery, []interface{}{offset, consumerGroup}
}

func (a DefaultMySQLBinlogOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=? FOR UPDATE
				), 0)`,
		[]interface{}{consumerGroup}
}

func (a DefaultMySQLBinlogOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("`watermill_offsets_%s`", topic)
}

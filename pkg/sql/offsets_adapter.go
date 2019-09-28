package sql

import "fmt"

type OffsetsAdapter interface {
	// AckMessageQuery the SQL query and arguments that will mark a message as read for a given consumer group.
	AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{})

	// ConsumedMessageQuery will return the SQL query and arguments which be executed after consuming message,
	// but before ack.
	//
	// ConsumedMessageQuery is optional, and will be not executed if query is empty.
	ConsumedMessageQuery(topic string, offset int, consumerGroup string, consumerULID []byte) (string, []interface{})

	// NextOffsetQuery returns the SQL query and arguments which should return offset of next message to consume.
	NextOffsetQuery(topic, consumerGroup string) (string, []interface{})

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []string
}

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
	// offset_consumed is not queried anywhere, it'a used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed)`
	return ackQuery, []interface{}{offset, consumerGroup}
}

// DefaultPostgresOffsetsAdapter is adapter for storing offsets in PostgreSQL database.
//
// DefaultPostgresOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We are using FOR UPDATE in NextOffsetQuery to lock consumer group in offsets table.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultPostgresOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultPostgresOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`
		CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset_acked BIGINT,
		offset_consumed BIGINT NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (a DefaultPostgresOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=$1 FOR UPDATE
				), 0)`,
		[]interface{}{consumerGroup}
}

func (a DefaultPostgresOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `UPDATE ` + a.MessagesOffsetsTable(topic) + ` SET offset_acked = $1 WHERE consumer_group = $2`
	return ackQuery, []interface{}{offset, consumerGroup}
}

func (a DefaultPostgresOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf(`"watermill_offsets_%s"`, topic)
}

func (a DefaultPostgresOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it'a used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES ($1, $2) ON CONFLICT("consumer_group") DO UPDATE SET offset_consumed=excluded.offset_consumed`
	return ackQuery, []interface{}{offset, consumerGroup}
}

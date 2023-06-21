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
		last_processed_transaction_id xid8 NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (a DefaultPostgreSQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `SELECT 
    			coalesce(MAX(offset_acked),0) AS offset_acked, 
    			coalesce(MAX(last_processed_transaction_id::text), '0')::xid8 AS last_processed_transaction_id 
			FROM ` + a.MessagesOffsetsTable(topic) + ` 
			WHERE consumer_group=$1`,
		[]interface{}{consumerGroup}
}

func (a DefaultPostgreSQLOffsetsAdapter) AckMessageQuery(topic string, row Row, consumerGroup string) (string, []interface{}) {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + `(offset_acked, last_processed_transaction_id, consumer_group) 
	VALUES 
		($1, $2, $3) 
	ON CONFLICT 
		(consumer_group) 
	DO UPDATE SET 
		offset_acked = excluded.offset_acked,
		last_processed_transaction_id = excluded.last_processed_transaction_id`
	return ackQuery, []interface{}{row.Offset, row.ExtraData["transaction_id"], consumerGroup}
}

func (a DefaultPostgreSQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf(`"watermill_offsets_%s"`, topic)
}

func (a DefaultPostgreSQLOffsetsAdapter) ConsumedMessageQuery(topic string, offset int64, consumerGroup string, consumerULID []byte) (string, []interface{}) {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	//ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
	//	VALUES ($1, $2) ON CONFLICT("consumer_group") DO UPDATE SET offset_consumed=excluded.offset_consumed`
	//return ackQuery, []interface{}{offset, consumerGroup}

	// todo: should be supported?
	return "", nil
}

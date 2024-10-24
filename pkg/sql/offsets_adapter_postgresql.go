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

func (a DefaultPostgreSQLOffsetsAdapter) SchemaInitializingQueries(params OffsetsSchemaInitializingQueriesParams) ([]Query, error) {
	return []Query{
		{
			Query: `
				CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(params.Topic) + ` (
				consumer_group VARCHAR(255) NOT NULL,
				offset_acked BIGINT,
				last_processed_transaction_id xid8 NOT NULL,
				PRIMARY KEY(consumer_group)
			)`,
		},
	}, nil
}

func (a DefaultPostgreSQLOffsetsAdapter) NextOffsetQuery(params NextOffsetQueryParams) (Query, error) {
	return Query{
		Query: `
			SELECT 
    			offset_acked, 
    			last_processed_transaction_id 
			FROM ` + a.MessagesOffsetsTable(params.Topic) + ` 
			WHERE consumer_group=$1 
			FOR UPDATE
		`,
		Args: []any{params.ConsumerGroup},
	}, nil
}

func (a DefaultPostgreSQLOffsetsAdapter) AckMessageQuery(params AckMessageQueryParams) (Query, error) {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(params.Topic) + `(offset_acked, last_processed_transaction_id, consumer_group) 
	VALUES 
		($1, $2, $3) 
	ON CONFLICT 
		(consumer_group) 
	DO UPDATE SET 
		offset_acked = excluded.offset_acked,
		last_processed_transaction_id = excluded.last_processed_transaction_id`

	return Query{ackQuery, []any{params.LastRow.Offset, params.LastRow.ExtraData["transaction_id"], params.ConsumerGroup}}, nil
}

func (a DefaultPostgreSQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf(`"watermill_offsets_%s"`, topic)
}

func (a DefaultPostgreSQLOffsetsAdapter) ConsumedMessageQuery(params ConsumedMessageQueryParams) (Query, error) {
	return Query{}, nil
}

func (a DefaultPostgreSQLOffsetsAdapter) BeforeSubscribingQueries(params BeforeSubscribingQueriesParams) ([]Query, error) {
	return []Query{
		{
			// It's required for exactly-once-delivery guarantee.
			// It adds "zero offsets" to the table with offsets.
			//
			// Without that, `FOR UDATE` from `NextOffsetQuery` won't work,
			// because there is nothing to lock.
			//
			// If "zero offsets" won't be present and multiple concurrent subscribers will try to consume them it
			// will lead to multiple delivery (because offsets are not locked).
			Query: `INSERT INTO ` + a.MessagesOffsetsTable(params.Topic) + ` (consumer_group, offset_acked, last_processed_transaction_id) VALUES ($1, 0, '0') ON CONFLICT DO NOTHING;`,
			Args:  []any{params.ConsumerGroup},
		},
	}, nil
}

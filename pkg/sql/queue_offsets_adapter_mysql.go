package sql

import (
	"fmt"
	"strings"
)

// MySQLQueueOffsetsAdapter is an OffsetsAdapter for the MySQLQueueSchema.
type MySQLQueueOffsetsAdapter struct {
	// DeleteOnAck determines whether the message should be deleted from the table when it is acknowledged.
	// If false, the message will be marked as acked.
	DeleteOnAck bool

	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
}

func (a MySQLQueueOffsetsAdapter) SchemaInitializingQueries(params OffsetsSchemaInitializingQueriesParams) ([]Query, error) {
	return []Query{}, nil
}

func (a MySQLQueueOffsetsAdapter) NextOffsetQuery(params NextOffsetQueryParams) (Query, error) {
	return Query{}, nil
}

func (a MySQLQueueOffsetsAdapter) AckMessageQuery(params AckMessageQueryParams) (Query, error) {
	if params.ConsumerGroup != "" {
		panic("consumer groups are not supported in MySQLQueueOffsetsAdapter")
	}

	var ackQuery string

	table := a.MessagesTable(params.Topic)

	if a.DeleteOnAck {
		// Build the WHERE clause for multiple offsets
		offsetPlaceholders := strings.Repeat("?,", len(params.Rows))
		offsetPlaceholders = strings.TrimRight(offsetPlaceholders, ",")
		ackQuery = fmt.Sprintf(`DELETE FROM %s WHERE `+"`offset`"+` IN (%s)`, table, offsetPlaceholders)
	} else {
		// Build the WHERE clause for multiple offsets
		offsetPlaceholders := strings.Repeat("?,", len(params.Rows))
		offsetPlaceholders = strings.TrimRight(offsetPlaceholders, ",")
		ackQuery = fmt.Sprintf(`UPDATE %s SET acked = TRUE WHERE `+"`offset`"+` IN (%s)`, table, offsetPlaceholders)
	}

	offsets := make([]any, len(params.Rows))
	for i, row := range params.Rows {
		offsets[i] = row.Offset
	}

	return Query{ackQuery, offsets}, nil
}

func (a MySQLQueueOffsetsAdapter) MessagesTable(topic string) string {
	if a.GenerateMessagesTableName != nil {
		return a.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("`watermill_%s`", topic)
}

func (a MySQLQueueOffsetsAdapter) ConsumedMessageQuery(params ConsumedMessageQueryParams) (Query, error) {
	return Query{}, nil
}

func (a MySQLQueueOffsetsAdapter) BeforeSubscribingQueries(params BeforeSubscribingQueriesParams) ([]Query, error) {
	return []Query{}, nil
}

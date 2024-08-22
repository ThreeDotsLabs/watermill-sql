package sql

import "fmt"

// ConditionalPostgreSQLOffsetsAdapter is an OffsetsAdapter for the ConditionalPostgreSQLSchema.
type ConditionalPostgreSQLOffsetsAdapter struct {
	// DeleteOnAck determines whether the message should be deleted from the table when it is acknowledged.
	// If false, the message will be marked as acked.
	DeleteOnAck bool

	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
}

func (a ConditionalPostgreSQLOffsetsAdapter) SchemaInitializingQueries(topic string) []Query {
	return []Query{}
}

func (a ConditionalPostgreSQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) Query {
	return Query{}
}

func (a ConditionalPostgreSQLOffsetsAdapter) AckMessageQuery(topic string, row Row, consumerGroup string) Query {
	if consumerGroup != "" {
		panic("consumer groups are not supported in ConditionalPostgreSQLOffsetsAdapter")
	}

	var ackQuery string

	table := a.GenerateMessagesTableName(topic)

	if a.DeleteOnAck {
		ackQuery = fmt.Sprintf("DELETE FROM %s WHERE offset = $1", table)
	} else {
		ackQuery = fmt.Sprintf("UPDATE %s SET acked = TRUE WHERE offset = $1", table)
	}

	return Query{ackQuery, []any{row.Offset}}
}

func (a ConditionalPostgreSQLOffsetsAdapter) MessagesTable(topic string) string {
	if a.GenerateMessagesTableName != nil {
		return a.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func (a ConditionalPostgreSQLOffsetsAdapter) ConsumedMessageQuery(topic string, row Row, consumerGroup string, consumerULID []byte) Query {
	return Query{}
}

func (a ConditionalPostgreSQLOffsetsAdapter) BeforeSubscribingQueries(topic string, consumerGroup string) []Query {
	return []Query{}
}

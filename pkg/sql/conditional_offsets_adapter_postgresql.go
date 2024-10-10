package sql

import (
	"fmt"
	"github.com/lib/pq"
)

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

func (a ConditionalPostgreSQLOffsetsAdapter) NextOffsetQuery(params NextOffsetQueryParams) Query {
	return Query{}
}

func (a ConditionalPostgreSQLOffsetsAdapter) AckMessageQuery(params AckMessageQueryParams) Query {
	if params.ConsumerGroup != "" {
		panic("consumer groups are not supported in ConditionalPostgreSQLOffsetsAdapter")
	}

	var ackQuery string

	table := a.MessagesTable(params.Topic)

	if a.DeleteOnAck {
		ackQuery = fmt.Sprintf(`DELETE FROM %s WHERE "offset" = ANY($1)`, table)
	} else {
		ackQuery = fmt.Sprintf(`UPDATE %s SET acked = TRUE WHERE "offset" = ANY($1)`, table)
	}

	offsets := make([]int64, len(params.Rows))
	for i, row := range params.Rows {
		offsets[i] = row.Offset
	}

	// TODO do we need to stick with pq?
	return Query{ackQuery, []any{pq.Array(offsets)}}
}

func (a ConditionalPostgreSQLOffsetsAdapter) MessagesTable(topic string) string {
	if a.GenerateMessagesTableName != nil {
		return a.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func (a ConditionalPostgreSQLOffsetsAdapter) ConsumedMessageQuery(params ConsumedMessageQueryParams) Query {
	return Query{}
}

func (a ConditionalPostgreSQLOffsetsAdapter) BeforeSubscribingQueries(topic string, consumerGroup string) []Query {
	return []Query{}
}

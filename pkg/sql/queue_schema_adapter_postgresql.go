package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

type GenerateWhereClauseParams struct {
	Topic string
}

// PostgreSQLQueueSchema is a schema adapter for PostgreSQL that allows filtering messages by some condition.
// It DOES NOT support consumer groups.
// It supports deleting messages on ack.
type PostgreSQLQueueSchema struct {
	// GenerateWhereClause is a function that returns a where clause and arguments for the SELECT query.
	// It may be used to filter messages by some condition.
	// If empty, no where clause will be added.
	GenerateWhereClause func(params GenerateWhereClauseParams) (string, []any)

	// GeneratePayloadType is the type of the payload column in the messages table.
	// By default, it's JSON. If your payload is not JSON, you can use BYTEA.
	GeneratePayloadType func(topic string) string

	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string

	// SubscribeBatchSize is the number of messages to be queried at once.
	//
	// Higher value, increases a chance of message re-delivery in case of crash or networking issues.
	// 1 is the safest value, but it may have a negative impact on performance when consuming a lot of messages.
	//
	// Default value is 100.
	SubscribeBatchSize int
}

func (s PostgreSQLQueueSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) []Query {
	createMessagesTable := ` 
		CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(params.Topic) + ` (
			"offset" SERIAL PRIMARY KEY,
			"uuid" VARCHAR(36) NOT NULL,
			"payload" ` + s.payloadColumnType(params.Topic) + ` DEFAULT NULL,
			"metadata" JSON DEFAULT NULL,
			"acked" BOOLEAN NOT NULL DEFAULT FALSE,
			"created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`

	return []Query{{Query: createMessagesTable}}
}

func (s PostgreSQLQueueSchema) InsertQuery(params InsertQueryParams) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(params.Topic),
		queueInsertMarkers(len(params.Msgs)),
	)

	args, err := defaultInsertArgs(params.Msgs)
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func queueInsertMarkers(count int) string {
	result := strings.Builder{}

	index := 1
	for i := 0; i < count; i++ {
		result.WriteString(fmt.Sprintf("($%d,$%d,$%d),", index, index+1, index+2))
		index += 3
	}

	return strings.TrimRight(result.String(), ",")
}

func (s PostgreSQLQueueSchema) batchSize() int {
	if s.SubscribeBatchSize == 0 {
		return 100
	}

	return s.SubscribeBatchSize
}

func (s PostgreSQLQueueSchema) SelectQuery(params SelectQueryParams) Query {
	if params.ConsumerGroup != "" {
		panic("consumer groups are not supported in PostgreSQLQueueSchema")
	}

	whereParams := GenerateWhereClauseParams{
		Topic: params.Topic,
	}

	var where string
	var args []any

	if s.GenerateWhereClause != nil {
		where, args = s.GenerateWhereClause(whereParams)
		if where != "" {
			where = "AND " + where
		}
	}

	selectQuery := `
		SELECT "offset", uuid, payload, metadata FROM ` + s.MessagesTable(params.Topic) + `
		WHERE acked = false ` + where + `
		ORDER BY
			"offset" ASC
		LIMIT ` + fmt.Sprintf("%d", s.batchSize()) + `
		FOR UPDATE`

	return Query{selectQuery, args}
}

func (s PostgreSQLQueueSchema) UnmarshalMessage(params UnmarshalMessageParams) (Row, error) {
	r := Row{}

	err := params.Row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return Row{}, fmt.Errorf("could not scan message row: %w", err)
	}

	msg := message.NewMessage(string(r.UUID), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return Row{}, fmt.Errorf("could not unmarshal metadata as JSON: %w", err)
		}
	}

	r.Msg = msg

	return r, nil
}

func (s PostgreSQLQueueSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func (s PostgreSQLQueueSchema) SubscribeIsolationLevel() sql.IsolationLevel {
	// For Postgres Repeatable Read is enough.
	return sql.LevelRepeatableRead
}

func (s PostgreSQLQueueSchema) payloadColumnType(topic string) string {
	if s.GeneratePayloadType == nil {
		return "JSON"
	}

	return s.GeneratePayloadType(topic)
}

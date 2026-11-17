package sql

import (
	"database/sql"
	"encoding/json"
	"errors"
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

	// BindJSONAsText makes the publisher bind the metadata, and the payload when the payload column type is JSON or JSONB,
	// as text instead of []byte.
	//
	// Enable it when the driver doesn't learn the parameter types from the server: pgx with default_query_exec_mode
	// set to exec or simple_protocol (for example, behind PgBouncer in transaction pooling mode).
	// Such a driver sends []byte as bytea, which a JSON column rejects with
	// "invalid input syntax for type json (SQLSTATE 22P02)".
	//
	// When enabled, the payload column type from GeneratePayloadType (JSON when not set) must match the real column,
	// also if the table is not created by this adapter, and the metadata column must be JSON or JSONB.
	// Text bound to a BYTEA column is parsed by PostgreSQL as bytea input, which changes backslash sequences
	// (`\101` is stored as `A`) and rejects NUL bytes.
	BindJSONAsText bool

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

func (s PostgreSQLQueueSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) ([]Query, error) {
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

	return []Query{{Query: createMessagesTable}}, nil
}

func (s PostgreSQLQueueSchema) InsertQuery(params InsertQueryParams) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(params.Topic),
		queueInsertMarkers(len(params.Msgs)),
	)

	args, err := defaultInsertArgs(params.Msgs, newJSONTextArgs(s.BindJSONAsText, s.payloadColumnType, params.Topic))
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func queueInsertMarkers(count int) string {
	result := strings.Builder{}

	index := 1
	for i := 0; i < count; i++ {
		fmt.Fprintf(&result, "($%d,$%d,$%d),", index, index+1, index+2)
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

func (s PostgreSQLQueueSchema) SelectQuery(params SelectQueryParams) (Query, error) {
	if params.ConsumerGroup != "" {
		return Query{}, errors.New("consumer groups are not supported in PostgreSQLQueueSchema")
	}

	whereParams := GenerateWhereClauseParams{
		Topic: params.Topic,
	}

	var where string
	var args []any

	if s.GenerateWhereClause != nil {
		where, args = s.GenerateWhereClause(whereParams)
		if where != "" {
			where = "AND (" + where + ")"
		}
	}

	selectQuery := `
		SELECT "offset", uuid, payload, metadata FROM ` + s.MessagesTable(params.Topic) + `
		WHERE acked = false ` + where + `
		ORDER BY
			"offset" ASC
		LIMIT ` + fmt.Sprintf("%d", s.batchSize()) + `
		FOR UPDATE`

	return Query{selectQuery, args}, nil
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

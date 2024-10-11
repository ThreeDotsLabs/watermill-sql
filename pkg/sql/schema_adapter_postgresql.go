package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultPostgreSQLSchema is a default implementation of SchemaAdapter based on PostgreSQL.
type DefaultPostgreSQLSchema struct {
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

func (s DefaultPostgreSQLSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) []Query {
	createMessagesTable := ` 
		CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(params.Topic) + ` (
			"offset" SERIAL,
			"uuid" VARCHAR(36) NOT NULL,
			"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			"payload" JSON DEFAULT NULL,
			"metadata" JSON DEFAULT NULL,
			"transaction_id" xid8 NOT NULL,
			PRIMARY KEY ("transaction_id", "offset")
		);
	`

	return []Query{{Query: createMessagesTable}}
}

func (s DefaultPostgreSQLSchema) InsertQuery(params InsertQueryParams) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata, transaction_id) VALUES %s`,
		s.MessagesTable(params.Topic),
		defaultInsertMarkers(len(params.Msgs)),
	)

	args, err := defaultInsertArgs(params.Msgs)
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func defaultInsertMarkers(count int) string {
	result := strings.Builder{}

	index := 1
	for i := 0; i < count; i++ {
		result.WriteString(fmt.Sprintf("($%d,$%d,$%d,pg_current_xact_id()),", index, index+1, index+2))
		index += 3
	}

	return strings.TrimRight(result.String(), ",")
}

func (s DefaultPostgreSQLSchema) batchSize() int {
	if s.SubscribeBatchSize == 0 {
		return 100
	}

	return s.SubscribeBatchSize
}

func (s DefaultPostgreSQLSchema) SelectQuery(params SelectQueryParams) Query {
	// Query inspired by https://event-driven.io/en/ordering_in_postgres_outbox/

	nextOffsetParams := NextOffsetQueryParams{
		Topic:         params.Topic,
		ConsumerGroup: params.ConsumerGroup,
	}

	nextOffsetQuery := params.OffsetsAdapter.NextOffsetQuery(nextOffsetParams)
	selectQuery := `
		WITH last_processed AS (
			` + nextOffsetQuery.Query + `
		)

		SELECT "offset", transaction_id, uuid, payload, metadata FROM ` + s.MessagesTable(params.Topic) + `

		WHERE 
		(
			(
				transaction_id = (SELECT last_processed_transaction_id FROM last_processed) 
				AND 
				"offset" > (SELECT offset_acked FROM last_processed)
			)
			OR
			(transaction_id > (SELECT last_processed_transaction_id FROM last_processed))
		)
		AND 
			transaction_id < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY
			transaction_id ASC,
			"offset" ASC
		LIMIT ` + fmt.Sprintf("%d", s.batchSize())

	return Query{selectQuery, nextOffsetQuery.Args}
}

func (s DefaultPostgreSQLSchema) UnmarshalMessage(params UnmarshalMessageParams) (Row, error) {
	r := Row{}
	var transactionID int64

	err := params.Row.Scan(&r.Offset, &transactionID, &r.UUID, &r.Payload, &r.Metadata)
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
	r.ExtraData = map[string]any{
		"transaction_id": transactionID,
	}

	return r, nil
}

func (s DefaultPostgreSQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func (s DefaultPostgreSQLSchema) SubscribeIsolationLevel() sql.IsolationLevel {
	// For Postgres Repeatable Read is enough.
	return sql.LevelRepeatableRead
}

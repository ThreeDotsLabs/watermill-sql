package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
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

	// AdvisoryXActLock if greater than zero will use pg_advisory_xact_lock to lock the transaction which is needed
	// to concurrently create tables. https://stackoverflow.com/questions/74261789/postgres-create-table-if-not-exists-%E2%87%92-23505
	AdvisoryXActLock int
}

func (s DefaultPostgreSQLSchema) SchemaInitializingQueries(topic string) []Query {
	createMessagesTable := ` 
		CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(topic) + ` (
			"offset" SERIAL,
			"uuid" VARCHAR(36) NOT NULL,
			"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			"payload" JSON DEFAULT NULL,
			"metadata" JSON DEFAULT NULL,
			"transaction_id" xid8 NOT NULL,
			PRIMARY KEY ("transaction_id", "offset")
		);
	`

	queries := []Query{{Query: createMessagesTable}}
	if s.AdvisoryXActLock > 0 {
		queries = append([]Query{
			{Query: fmt.Sprintf("SELECT pg_advisory_xact_lock(%d);", s.AdvisoryXActLock)},
		}, queries...)
	}

	return queries
}

func (s DefaultPostgreSQLSchema) InsertQuery(topic string, msgs message.Messages) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata, transaction_id) VALUES %s`,
		s.MessagesTable(topic),
		defaultInsertMarkers(len(msgs)),
	)

	args, err := defaultInsertArgs(msgs)
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

func (s DefaultPostgreSQLSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) Query {
	// Query inspired by https://event-driven.io/en/ordering_in_postgres_outbox/

	nextOffsetQuery := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)
	selectQuery := `
		WITH last_processed AS (
			` + nextOffsetQuery.Query + `
		)

		SELECT "offset", transaction_id, uuid, payload, metadata FROM ` + s.MessagesTable(topic) + `

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

func (s DefaultPostgreSQLSchema) UnmarshalMessage(row Scanner) (Row, error) {
	r := Row{}
	var transactionID int64

	err := row.Scan(&r.Offset, &transactionID, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return Row{}, errors.Wrap(err, "could not scan message row")
	}

	msg := message.NewMessage(string(r.UUID), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return Row{}, errors.Wrap(err, "could not unmarshal metadata as JSON")
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

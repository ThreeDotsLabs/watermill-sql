package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultPostgreSQLSchema is a default implementation of SchemaAdapter based on PostgreSQL.
type DefaultPostgreSQLSchema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string

	// GeneratePayloadType is the type of the payload column in the messages table.
	// By default, it's JSON. If your payload is not JSON, you can use BYTEA.
	GeneratePayloadType func(topic string) string

	// SubscribeBatchSize is the number of messages to be queried at once.
	//
	// Higher value, increases a chance of message re-delivery in case of crash or networking issues.
	// 1 is the safest value, but it may have a negative impact on performance when consuming a lot of messages.
	//
	// Default value is 100.
	SubscribeBatchSize int

	// InitializeSchemaInTransaction determines if the schema should be initialized in a transaction.
	// By default, it happens in a transaction due to the following:
	// https://stackoverflow.com/questions/74261789/postgres-create-table-if-not-exists-%E2%87%92-23505
	// Flip this to false if you want to initialize the schema without a transaction.
	InitializeSchemaWithoutTransaction bool

	// InitializeSchemaLock is a PostgreSQL advisory lock to be acquired before initializing the schema.
	// If empty and InitializeSchemaWithoutTransaction is false, a default will be used.
	InitializeSchemaLock int
}

func (s DefaultPostgreSQLSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) ([]Query, error) {
	// theoretically this primary key allows duplicate offsets for same transaction_id
	// but in practice it should not happen with SERIAL, so we can keep one index and make it more
	// storage efficient
	//
	// it's intended that transaction_id is first in the index, because we are using it alone
	// in the WHERE clause
	createMessagesTable := ` 
		CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(params.Topic) + ` (
			"offset" BIGSERIAL,
			"uuid" VARCHAR(36) NOT NULL,
			"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			"payload" ` + s.PayloadColumnType(params.Topic) + ` DEFAULT NULL,
			"metadata" JSON DEFAULT NULL,
			"transaction_id" xid8 NOT NULL,
			PRIMARY KEY ("transaction_id", "offset")
		);
	`

	queries := []Query{{Query: createMessagesTable}}
	if !s.InitializeSchemaWithoutTransaction {
		lock := DefaultSchemaInitializationLock("watermill")
		if s.InitializeSchemaLock > 0 {
			lock = s.InitializeSchemaLock
		}

		queries = append([]Query{
			{Query: fmt.Sprintf("SELECT pg_advisory_xact_lock(%d);", lock)},
		}, queries...)
	}

	return queries, nil
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

func (s DefaultPostgreSQLSchema) SelectQuery(params SelectQueryParams) (Query, error) {
	// Query inspired by https://event-driven.io/en/ordering_in_postgres_outbox/

	nextOffsetParams := NextOffsetQueryParams{
		Topic:         params.Topic,
		ConsumerGroup: params.ConsumerGroup,
	}

	nextOffsetQuery, err := params.OffsetsAdapter.NextOffsetQuery(nextOffsetParams)
	if err != nil {
		return Query{}, err
	}

	// We are using subquery to avoid problems with query planner mis-estimating
	// and performing expensive index scans, read more:
	// - https://pganalyze.com/blog/5mins-postgres-planner-order-by-limit
	// - https://pganalyze.com/docs/explain/insights/mis-estimate
	//
	// Example slow query plan:
	//
	// Limit  (cost=8.65..8.83 rows=1 width=32) (actual time=184.350..184.353 rows=1 loops=1)
	//  CTE last_processed
	//    ->  LockRows  (cost=0.14..8.17 rows=1 width=22) (actual time=1.572..1.574 rows=1 loops=1)
	//          ->  Index Scan using <table name>_offsets_pkey on <table name>_offsets  (cost=0.14..8.16 rows=1 width=22) (actual time=1.418..1.419 rows=1 loops=1)
	//                Index Cond: ((consumer_group)::text = ''::text)
	//  InitPlan 2 (returns $2)
	//    ->  CTE Scan on last_processed  (cost=0.00..0.02 rows=1 width=8) (actual time=1.583..1.585 rows=1 loops=1)
	//  InitPlan 3 (returns $3)
	//    ->  CTE Scan on last_processed last_processed_1  (cost=0.00..0.02 rows=1 width=8) (actual time=0.006..0.007 rows=1 loops=1)
	//  InitPlan 4 (returns $4)
	//    ->  CTE Scan on last_processed last_processed_2  (cost=0.00..0.02 rows=1 width=8) (actual time=0.000..0.001 rows=1 loops=1)
	//  ->  Index Scan using <index name> on <table name>  (cost=0.42..14462.65 rows=80423 width=32) (actual time=184.348..184.348 rows=1 loops=1)
	//        Index Cond: (transaction_id < pg_snapshot_xmin(pg_current_snapshot()))
	//        Filter: (((transaction_id = $2) AND (""offset"" > $3)) OR (transaction_id > $4))"
	//        Rows Removed by Filter: 241157
	//  Planning Time: 8.242 ms
	//  Execution Time: 185.214 ms
	//
	// Example performant query plan:
	// Limit  (cost=3138.06..3138.06 rows=1 width=32) (actual time=0.579..0.580 rows=1 loops=1)
	//  ->  Sort  (cost=3138.06..3339.11 rows=80423 width=32) (actual time=0.577..0.579 rows=1 loops=1)
	//        Sort Key: <table name>.transaction_id, <table name>.""offset"""
	//        Sort Method: top-N heapsort  Memory: 25kB
	//        ->  Bitmap Heap Scan on <table name>  (cost=85.36..1931.71 rows=80423 width=32) (actual time=0.231..0.530 rows=112 loops=1)
	//              Recheck Cond: (((transaction_id = $2) AND (transaction_id < pg_snapshot_xmin(pg_current_snapshot())) AND (""offset"" > $3)) OR ((transaction_id > $4) AND (transaction_id < pg_snapshot_xmin(pg_current_snapshot()))))"
	//              Heap Blocks: exact=26
	//              CTE last_processed
	//                ->  LockRows  (cost=0.14..8.17 rows=1 width=22) (actual time=0.186..0.188 rows=1 loops=1)
	//                      ->  Index Scan using <table name>_offsets_pkey on <table name>_offsets  (cost=0.14..8.16 rows=1 width=22) (actual time=0.033..0.035 rows=1 loops=1)
	//                            Index Cond: ((consumer_group)::text = ''::text)
	//              InitPlan 2 (returns $2)
	//                ->  CTE Scan on last_processed  (cost=0.00..0.02 rows=1 width=8) (actual time=0.189..0.191 rows=1 loops=1)
	//              InitPlan 3 (returns $3)
	//                ->  CTE Scan on last_processed last_processed_1  (cost=0.00..0.02 rows=1 width=8) (actual time=0.000..0.000 rows=1 loops=1)
	//              InitPlan 4 (returns $4)
	//                ->  CTE Scan on last_processed last_processed_2  (cost=0.00..0.02 rows=1 width=8) (actual time=0.000..0.000 rows=1 loops=1)
	//              ->  BitmapOr  (cost=77.12..77.12 rows=1207 width=0) (actual time=0.219..0.219 rows=0 loops=1)
	//                    ->  Bitmap Index Scan on <index name>  (cost=0.00..4.43 rows=1 width=0) (actual time=0.208..0.208 rows=0 loops=1)
	//                          Index Cond: ((transaction_id = $2) AND (transaction_id < pg_snapshot_xmin(pg_current_snapshot())) AND (""offset"" > $3))"
	//                    ->  Bitmap Index Scan on <index name>  (cost=0.00..32.48 rows=1206 width=0) (actual time=0.010..0.011 rows=112 loops=1)
	//                          Index Cond: ((transaction_id > $4) AND (transaction_id < pg_snapshot_xmin(pg_current_snapshot())))
	// Planning Time: 1.365 ms
	// Execution Time: 0.786 ms

	selectQuery := `
	SELECT * FROM (
		WITH last_processed AS (
			` + nextOffsetQuery.Query + `
		)

		SELECT "offset", transaction_id::text, uuid, payload, metadata FROM ` + s.MessagesTable(params.Topic) + `

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
	) AS messages
	ORDER BY
		transaction_id ASC,
		"offset" ASC
	LIMIT ` + fmt.Sprintf("%d", s.batchSize())

	return Query{selectQuery, nextOffsetQuery.Args}, nil
}

func (s DefaultPostgreSQLSchema) UnmarshalMessage(params UnmarshalMessageParams) (Row, error) {
	r := Row{}
	var transactionID XID8

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
		"transaction_id": uint64(transactionID),
	}

	return r, nil
}

func (s DefaultPostgreSQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func (s DefaultPostgreSQLSchema) PayloadColumnType(topic string) string {
	if s.GeneratePayloadType == nil {
		return "JSON"
	}

	return s.GeneratePayloadType(topic)
}

func (s DefaultPostgreSQLSchema) SubscribeIsolationLevel() sql.IsolationLevel {
	// For Postgres Repeatable Read is enough.
	return sql.LevelRepeatableRead
}

func (s DefaultPostgreSQLSchema) RequiresTransaction() bool {
	return !s.InitializeSchemaWithoutTransaction
}

func DefaultSchemaInitializationLock(appName string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte("schema_init:" + appName))

	// Use only the lower 31 bits to ensure the number is positive
	// PostgreSQL advisory locks use int32 internally
	return int(h.Sum32() & 0x7fffffff)
}

type XID8 uint64

func (x *XID8) Scan(src interface{}) error {
	if src == nil {
		return errors.New("cannot scan nil value into XID8")
	}

	switch v := src.(type) {
	case string:
		val, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		*x = XID8(val)
		return nil
	case []byte:
		val, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}
		*x = XID8(val)
		return nil
	default:
		return errors.New("unsupported Scan value type for XID8")
	}
}

func (x *XID8) Value() (driver.Value, error) {
	return uint64(*x), nil
}

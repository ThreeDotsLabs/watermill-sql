package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultMySQLSchema is a default implementation of SchemaAdapter based on MySQL.
// If you need some customization, you can use composition to change schema and method of unmarshaling.
//
//	type MyMessagesSchema struct {
//		DefaultMySQLSchema
//	}
//
//	func (m MyMessagesSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) []string {
//		createMessagesTable := strings.Join([]string{
//			"CREATE TABLE IF NOT EXISTS " + m.MessagesTable(topic) + " (",
//			"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
//			"`uuid` BINARY(16) NOT NULL,",
//			"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
//			"`payload` JSON DEFAULT NULL,",
//			"`metadata` JSON DEFAULT NULL",
//			");",
//		}, "\n")
//
//		return []string{createMessagesTable}
//	}
//
//	func (m MyMessagesSchema) UnmarshalMessage(params UnmarshalMessageParams) (offset int, msg *message.Message, err error) {
//		// ...
//
// For debugging your custom schema, we recommend to inject logger with trace logging level
// which will print all SQL queries.
type DefaultMySQLSchema struct {
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
}

func (s DefaultMySQLSchema) SchemaInitializingQueries(params SchemaInitializingQueriesParams) ([]Query, error) {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(params.Topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(36) NOT NULL,",
		"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		"`payload` " + s.PayloadColumnType(params.Topic) + " DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")

	return []Query{{Query: createMessagesTable}}, nil
}

func (s DefaultMySQLSchema) InsertQuery(params InsertQueryParams) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(params.Topic),
		strings.TrimRight(strings.Repeat(`(?,?,?),`, len(params.Msgs)), ","),
	)

	args, err := defaultInsertArgs(params.Msgs)
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func (s DefaultMySQLSchema) batchSize() int {
	if s.SubscribeBatchSize == 0 {
		return 100
	}

	return s.SubscribeBatchSize
}

func (s DefaultMySQLSchema) SelectQuery(params SelectQueryParams) (Query, error) {
	nextOffsetParams := NextOffsetQueryParams{
		Topic:         params.Topic,
		ConsumerGroup: params.ConsumerGroup,
	}
	nextOffsetQuery, err := params.OffsetsAdapter.NextOffsetQuery(nextOffsetParams)
	if err != nil {
		return Query{}, err
	}

	// It's important to wrap offset with "`" for MariaDB.
	// See https://github.com/ThreeDotsLabs/watermill/issues/377
	selectQuery := "SELECT `offset`, `uuid`, `payload`, `metadata` FROM " + s.MessagesTable(params.Topic) +
		" WHERE `offset` > (" + nextOffsetQuery.Query + ") ORDER BY `offset` ASC" +
		` LIMIT ` + fmt.Sprintf("%d", s.batchSize())

	return Query{Query: selectQuery, Args: nextOffsetQuery.Args}, nil
}

func (s DefaultMySQLSchema) UnmarshalMessage(params UnmarshalMessageParams) (Row, error) {
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

func (s DefaultMySQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("`watermill_%s`", topic)
}

func (s DefaultMySQLSchema) PayloadColumnType(topic string) string {
	if s.GeneratePayloadType == nil {
		return "JSON"
	}

	return s.GeneratePayloadType(topic)
}

func (s DefaultMySQLSchema) SubscribeIsolationLevel() sql.IsolationLevel {
	// MySQL requires serializable isolation level for not losing messages.
	return sql.LevelSerializable
}

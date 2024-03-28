package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// DefaultMySQLSchema is a default implementation of SchemaAdapter based on MySQL.
// If you need some customization, you can use composition to change schema and method of unmarshaling.
//
//	type MyMessagesSchema struct {
//		DefaultMySQLSchema
//	}
//
//	func (m MyMessagesSchema) SchemaInitializingQueries(topic string) []string {
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
//	func (m MyMessagesSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
//		// ...
//
// For debugging your custom schema, we recommend to inject logger with trace logging level
// which will print all SQL queries.
type DefaultMySQLSchema struct {
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

func (s DefaultMySQLSchema) SchemaInitializingQueries(topic string) []Query {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(36) NOT NULL,",
		"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		"`payload` JSON DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")

	return []Query{{Query: createMessagesTable}}
}

func (s DefaultMySQLSchema) InsertQuery(topic string, msgs message.Messages) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		strings.TrimRight(strings.Repeat(`(?,?,?),`, len(msgs)), ","),
	)

	args, err := defaultInsertArgs(msgs)
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

func (s DefaultMySQLSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) Query {
	nextOffsetQuery := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)

	// It's important to wrap offset with "`" for MariaDB.
	// See https://github.com/ThreeDotsLabs/watermill/issues/377
	selectQuery := "SELECT `offset`, `uuid`, `payload`, `metadata` FROM " + s.MessagesTable(topic) +
		" WHERE `offset` > (" + nextOffsetQuery.Query + ") ORDER BY `offset` ASC" +
		` LIMIT ` + fmt.Sprintf("%d", s.batchSize())

	return Query{Query: selectQuery, Args: nextOffsetQuery.Args}
}

func (s DefaultMySQLSchema) UnmarshalMessage(row Scanner) (Row, error) {
	r := Row{}
	err := row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
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

	return r, nil
}

func (s DefaultMySQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("`watermill_%s`", topic)
}

func (s DefaultMySQLSchema) SubscribeIsolationLevel() sql.IsolationLevel {
	// MySQL requires serializable isolation level for not losing messages.
	return sql.LevelSerializable
}

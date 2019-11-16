package sql

import (
	"database/sql"
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
	// WithDB is the database handle that should be used instead of the calling publisher/subscriber's handle.
	// The publisher or subscriber's handle is used if WithDB is nil.
	// If your publisher or subscriber is configured to automatically initialize schema AND uses a sql.Tx or equivalent as its handle,
	// you want to provide a separate handle for the Schema Adapter, because CREATE TABLE requests cause implicit commits:
	// https://mariadb.com/kb/en/library/sql-statements-that-cause-an-implicit-commit/
	WithDB *sql.DB
}

func (s DefaultMySQLSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(36) NOT NULL,",
		"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		"`payload` JSON DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")

	return []string{createMessagesTable}
}

func (s DefaultMySQLSchema) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		strings.TrimRight(strings.Repeat(`(?,?,?),`, len(msgs)), ","),
	)

	args, err := defaultInsertArgs(msgs)
	if err != nil {
		return "", nil, err
	}

	return insertQuery, args, nil
}

func (s DefaultMySQLSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{}) {
	nextOffsetQuery, nextOffsetArgs := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)
	selectQuery := `
		SELECT offset, uuid, payload, metadata FROM ` + s.MessagesTable(topic) + `
		WHERE 
			offset > (` + nextOffsetQuery + `)
		ORDER BY 
			offset ASC
		LIMIT 1`

	return selectQuery, nextOffsetArgs
}

func (s DefaultMySQLSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	return unmarshalDefaultMessage(row)
}

func (s DefaultMySQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("`watermill_%s`", topic)
}

func (s DefaultMySQLSchema) DB() *sql.DB {
	return s.WithDB
}

package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// SchemaAdapter produces the SQL queries and arguments appropriately for a specific schema and dialect
// It also transforms sql.Rows into Watermill messages.
type SchemaAdapter interface {
	// InsertQuery returns the SQL query and arguments that will insert the Watermill message into the SQL storage.
	InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error)

	// SelectQuery returns the the SQL query and arguments
	// that returns the next unread message for a given consumer group.
	SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{})

	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	// It also returns the offset of the last read message, for the purpose of acking.
	UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error)

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []string
}

type defaultSchemaRow struct {
	Offset   int64
	UUID     []byte
	Payload  []byte
	Metadata []byte
}

func defaultInsertArgs(msgs message.Messages) ([]interface{}, error) {
	var args []interface{}
	for _, msg := range msgs {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal metadata into JSON for message %s", msg.UUID)
		}

		args = append(args, msg.UUID, msg.Payload, metadata)
	}

	return args, nil
}

func unmarshalDefaultMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	r := defaultSchemaRow{}
	err = row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return 0, nil, errors.Wrap(err, "could not scan message row")
	}

	msg = message.NewMessage(string(r.UUID), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return 0, nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return int(r.Offset), msg, nil
}

// Deprecated: Use DefaultMySQLSchema instead.
type DefaultSchema = DefaultMySQLSchema

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

// DefaultPostgresSchema is a default implementation of SchemaAdapter based on PostgreSQL.
type DefaultPostgresSchema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
}

func (s DefaultPostgresSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		`CREATE TABLE IF NOT EXISTS ` + s.MessagesTable(topic) + ` (`,
		`"offset" SERIAL,`,
		`"uuid" VARCHAR(36) NOT NULL,`,
		`"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,`,
		`"payload" JSON DEFAULT NULL,`,
		`"metadata" JSON DEFAULT NULL`,
		`);`,
	}, "\n")

	return []string{createMessagesTable}
}

func (s DefaultPostgresSchema) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		strings.TrimRight(strings.Repeat(`($1,$2,$3),`, len(msgs)), ","),
	)

	args, err := defaultInsertArgs(msgs)
	if err != nil {
		return "", nil, err
	}

	return insertQuery, args, nil
}

func (s DefaultPostgresSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{}) {
	nextOffsetQuery, nextOffsetArgs := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)
	selectQuery := `
		SELECT "offset", uuid, payload, metadata FROM ` + s.MessagesTable(topic) + `
		WHERE
			"offset" > (` + nextOffsetQuery + `)
		ORDER BY
			"offset" ASC
		LIMIT 1`

	return selectQuery, nextOffsetArgs
}

func (s DefaultPostgresSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	return unmarshalDefaultMessage(row)
}

func (s DefaultPostgresSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

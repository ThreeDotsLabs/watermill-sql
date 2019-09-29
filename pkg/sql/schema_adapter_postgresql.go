package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultPostgreSQLSchema is a default implementation of SchemaAdapter based on PostgreSQL.
type DefaultPostgreSQLSchema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
}

func (s DefaultPostgreSQLSchema) SchemaInitializingQueries(topic string) []string {
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

func (s DefaultPostgreSQLSchema) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
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

func (s DefaultPostgreSQLSchema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{}) {
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

func (s DefaultPostgreSQLSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	return unmarshalDefaultMessage(row)
}

func (s DefaultPostgreSQLSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

package sql_test

import (
	"strings"

	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
)

// testMySQLSchema makes the following changes to DefaultMySQLSchema to comply with tests:
// - uuid is a VARCHAR(255) instead of VARCHAR(36); some UUIDs in tests are bigger and we don't care for storage use
// - payload is a VARBINARY(255) instead of JSON; tests don't presuppose JSON-marshallable payloads
type testMySQLSchema struct {
	sql.DefaultMySQLSchema
}

func (s *testMySQLSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(255) NOT NULL,",
		"`payload` VARBINARY(255) DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")

	return []string{createMessagesTable}
}

type testPostgresSchema struct {
	sql.DefaultPostgresSchema
}

func (s *testPostgresSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		`"offset" SERIAL,`,
		`"uuid" VARCHAR(255) NOT NULL,`,
		`"payload" bytea DEFAULT NULL,`,
		`"metadata" JSON DEFAULT NULL`,
		`);`,
	}, "\n")

	return []string{createMessagesTable}
}

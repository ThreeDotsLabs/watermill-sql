package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestInsertQuery_json_arguments_bound_as_bytes_by_default(t *testing.T) {
	testCases := []struct {
		Name          string
		SchemaAdapter SchemaAdapter
	}{
		{Name: "postgresql_default_schema", SchemaAdapter: DefaultPostgreSQLSchema{}},
		{Name: "postgresql_default_schema_json", SchemaAdapter: DefaultPostgreSQLSchema{GeneratePayloadType: payloadType("JSON")}},
		{Name: "postgresql_queue_schema", SchemaAdapter: PostgreSQLQueueSchema{}},
		{Name: "mysql_default_schema", SchemaAdapter: DefaultMySQLSchema{}},
		{Name: "mysql_queue_schema", SchemaAdapter: MySQLQueueSchema{}},
		{Name: "mysql_delayed_schema", SchemaAdapter: delayedMySQLSchemaAdapter{}},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			args := insertQueryArgs(t, tc.SchemaAdapter)

			// assert.Equal with an expected []byte requires the actual value to be []byte as well.
			assert.Equal(t, []byte(`{"json": "field"}`), args.withPayload[1])
			assert.Equal(t, []byte(`{"key":"value"}`), args.withPayload[2])
		})
	}
}

func TestInsertQuery_bind_json_as_text(t *testing.T) {
	testCases := []struct {
		Name                string
		SchemaAdapter       SchemaAdapter
		ExpectPayloadAsText bool
	}{
		{Name: "postgresql_default_schema_default_type", SchemaAdapter: DefaultPostgreSQLSchema{BindJSONAsText: true}, ExpectPayloadAsText: true},
		{Name: "postgresql_default_schema_json", SchemaAdapter: DefaultPostgreSQLSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("JSON")}, ExpectPayloadAsText: true},
		{Name: "postgresql_default_schema_jsonb", SchemaAdapter: DefaultPostgreSQLSchema{BindJSONAsText: true, GeneratePayloadType: payloadType(" jsonb ")}, ExpectPayloadAsText: true},
		{Name: "postgresql_default_schema_bytea", SchemaAdapter: DefaultPostgreSQLSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("BYTEA")}, ExpectPayloadAsText: false},
		{Name: "postgresql_default_schema_custom_type", SchemaAdapter: DefaultPostgreSQLSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("my_json_domain")}, ExpectPayloadAsText: false},
		{Name: "postgresql_queue_schema_default_type", SchemaAdapter: PostgreSQLQueueSchema{BindJSONAsText: true}, ExpectPayloadAsText: true},
		{Name: "postgresql_queue_schema_bytea", SchemaAdapter: PostgreSQLQueueSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("bytea")}, ExpectPayloadAsText: false},
		{Name: "mysql_default_schema_default_type", SchemaAdapter: DefaultMySQLSchema{BindJSONAsText: true}, ExpectPayloadAsText: true},
		{Name: "mysql_default_schema_longblob", SchemaAdapter: DefaultMySQLSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("LONGBLOB")}, ExpectPayloadAsText: false},
		{Name: "mysql_queue_schema_default_type", SchemaAdapter: MySQLQueueSchema{BindJSONAsText: true}, ExpectPayloadAsText: true},
		{Name: "mysql_queue_schema_varbinary", SchemaAdapter: MySQLQueueSchema{BindJSONAsText: true, GeneratePayloadType: payloadType("VARBINARY(255)")}, ExpectPayloadAsText: false},
		{Name: "mysql_delayed_schema_default_type", SchemaAdapter: delayedMySQLSchemaAdapter{MySQLQueueSchema{BindJSONAsText: true}}, ExpectPayloadAsText: true},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			args := insertQueryArgs(t, tc.SchemaAdapter)

			if tc.ExpectPayloadAsText {
				assert.Equal(t, `{"json": "field"}`, args.withPayload[1])
			} else {
				assert.Equal(t, []byte(`{"json": "field"}`), args.withPayload[1])
			}
			assert.Equal(t, `{"key":"value"}`, args.withPayload[2])

			// A nil payload is stored as NULL only if it's bound as a nil []byte, not as an empty string.
			assert.IsType(t, []byte(nil), args.withoutPayload[1])
			assert.Nil(t, args.withoutPayload[1])
		})
	}
}

type testInsertArgs struct {
	withPayload    []any
	withoutPayload []any
}

func insertQueryArgs(t *testing.T, schemaAdapter SchemaAdapter) testInsertArgs {
	t.Helper()

	withPayload := message.NewMessage("1", message.Payload(`{"json": "field"}`))
	withPayload.Metadata.Set("key", "value")
	withoutPayload := message.NewMessage("2", nil)

	query, err := schemaAdapter.InsertQuery(InsertQueryParams{
		Topic: "topic",
		Msgs:  message.Messages{withPayload, withoutPayload},
	})
	require.NoError(t, err)

	argsPerMessage := len(query.Args) / 2
	require.GreaterOrEqual(t, argsPerMessage, 3)

	return testInsertArgs{
		withPayload:    query.Args[:argsPerMessage],
		withoutPayload: query.Args[argsPerMessage:],
	}
}

func payloadType(columnType string) func(topic string) string {
	return func(topic string) string {
		return columnType
	}
}

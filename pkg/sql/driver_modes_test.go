package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Written in the form in which MySQL stores JSON documents (MySQL re-serializes JSON),
// so the round trip is byte-identical on every database.
var jsonColumnPayloads = []message.Payload{
	message.Payload(`{"q": "it's \"x\" \\ y", "text": "zażółć gęślą jaźń 日本語 🎉"}`),
	message.Payload(`"żółć ąę €"`),
	nil,
}

// A text-format bytea parser would reject or alter these, and NULL must stay distinct from an empty payload.
var binaryColumnPayloads = []message.Payload{
	{0x00, 0x01, 0xff, 0xfe, 0x80, 'a', 0xc3, 0x28, 0x00},
	message.Payload(`C:\\dir \101 zażółć`),
	nil,
	{},
}

type driverModesDB struct {
	Name string
	DB   sql.Beginner

	// Modes that don't learn the parameter types from the server (pgx exec and simple_protocol,
	// MySQL interpolateParams=true) send []byte as binary data, which JSON columns reject.
	KnowsParameterTypes bool
}

// Without BindJSONAsText, publishing fails in the modes that don't know the parameter types.
var bindJSONAsTextOptions = []struct {
	Name                        string
	BindJSONAsText              bool
	RequiresKnownParameterTypes bool
}{
	{Name: "bytes", BindJSONAsText: false, RequiresKnownParameterTypes: true},
	{Name: "bind_json_as_text", BindJSONAsText: true},
}

// pgx query exec modes that don't ask the server for parameter types (exec and simple_protocol) are used
// e.g. behind PgBouncer in transaction pooling mode.
func TestPostgreSQLDriverModes_publish_and_receive(t *testing.T) {
	t.Parallel()

	dbs := []driverModesDB{
		{Name: "lib_pq", DB: newPostgreSQL(t), KnowsParameterTypes: true},
		{Name: "pgx_stdlib_default", DB: newPgxPostgreSQL(t), KnowsParameterTypes: true},
		{Name: "pgx_stdlib_exec", DB: newPgxStdlibWithQueryExecMode(t, "exec")},
		{Name: "pgx_stdlib_simple_protocol", DB: newPgxStdlibWithQueryExecMode(t, "simple_protocol")},
		{Name: "pgx_default", DB: newPgx(t), KnowsParameterTypes: true},
		{Name: "pgx_exec", DB: newPgxWithQueryExecMode(t, "exec")},
		{Name: "pgx_simple_protocol", DB: newPgxWithQueryExecMode(t, "simple_protocol")},
	}

	payloadColumns := []struct {
		Name                string
		GeneratePayloadType func(topic string) string
		Payloads            []message.Payload
	}{
		{Name: "default_json_payload", GeneratePayloadType: nil, Payloads: jsonColumnPayloads},
		{Name: "declared_json_payload", GeneratePayloadType: payloadType("JSON"), Payloads: jsonColumnPayloads},
		{Name: "declared_bytea_payload", GeneratePayloadType: payloadType("BYTEA"), Payloads: binaryColumnPayloads},
	}

	type schemaOptions struct {
		GeneratePayloadType func(topic string) string
		BindJSONAsText      bool
	}

	schemas := []struct {
		Name     string
		Adapters func(opts schemaOptions) (sql.SchemaAdapter, sql.OffsetsAdapter)
	}{
		{
			Name: "default_schema",
			Adapters: func(opts schemaOptions) (sql.SchemaAdapter, sql.OffsetsAdapter) {
				return sql.DefaultPostgreSQLSchema{
					GeneratePayloadType:  opts.GeneratePayloadType,
					BindJSONAsText:       opts.BindJSONAsText,
					InitializeSchemaLock: rand.Intn(1000000),
				}, sql.DefaultPostgreSQLOffsetsAdapter{}
			},
		},
		{
			Name: "queue_schema",
			Adapters: func(opts schemaOptions) (sql.SchemaAdapter, sql.OffsetsAdapter) {
				return sql.PostgreSQLQueueSchema{
					GeneratePayloadType: opts.GeneratePayloadType,
					BindJSONAsText:      opts.BindJSONAsText,
				}, sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}
			},
		},
	}

	for _, db := range dbs {
		for _, bind := range bindJSONAsTextOptions {
			if bind.RequiresKnownParameterTypes && !db.KnowsParameterTypes {
				continue
			}

			for _, schema := range schemas {
				for _, payloadColumn := range payloadColumns {
					t.Run(db.Name+"/"+bind.Name+"/"+schema.Name+"/"+payloadColumn.Name, func(t *testing.T) {
						t.Parallel()

						schemaAdapter, offsetsAdapter := schema.Adapters(schemaOptions{
							GeneratePayloadType: payloadColumn.GeneratePayloadType,
							BindJSONAsText:      bind.BindJSONAsText,
						})
						pub, sub := newDriverModesPubSub(t, db.DB, schemaAdapter, offsetsAdapter)

						testPublishAndReceiveAll(t, pub, sub, payloadColumn.Payloads)
					})
				}
			}
		}
	}
}

func TestMySQLDriverModes_publish_and_receive(t *testing.T) {
	t.Parallel()

	dbs := []driverModesDB{
		{Name: "default", DB: newMySQL(t), KnowsParameterTypes: true},
		{Name: "interpolate_params", DB: newMySQLWithInterpolateParams(t)},
	}

	pubSubs := []struct {
		Name      string
		NewPubSub func(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber)
		Payloads  []message.Payload
	}{
		{
			Name: "default_schema/default_json_payload",
			NewPubSub: func(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber) {
				schemaAdapter := sql.DefaultMySQLSchema{BindJSONAsText: bindJSONAsText}
				return newDriverModesPubSub(t, db, schemaAdapter, sql.DefaultMySQLOffsetsAdapter{})
			},
			Payloads: jsonColumnPayloads,
		},
		{
			Name: "default_schema/declared_longblob_payload",
			NewPubSub: func(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber) {
				schemaAdapter := sql.DefaultMySQLSchema{GeneratePayloadType: payloadType("LONGBLOB"), BindJSONAsText: bindJSONAsText}
				return newDriverModesPubSub(t, db, schemaAdapter, sql.DefaultMySQLOffsetsAdapter{})
			},
			Payloads: binaryColumnPayloads,
		},
		{
			Name: "queue_schema/default_json_payload",
			NewPubSub: func(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber) {
				schemaAdapter := sql.MySQLQueueSchema{BindJSONAsText: bindJSONAsText}
				return newDriverModesPubSub(t, db, schemaAdapter, sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true})
			},
			Payloads: jsonColumnPayloads,
		},
		{
			Name: "queue_schema/declared_longblob_payload",
			NewPubSub: func(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber) {
				schemaAdapter := sql.MySQLQueueSchema{GeneratePayloadType: payloadType("LONGBLOB"), BindJSONAsText: bindJSONAsText}
				return newDriverModesPubSub(t, db, schemaAdapter, sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true})
			},
			Payloads: binaryColumnPayloads,
		},
		{
			Name:      "delayed/default_json_payload",
			NewPubSub: newDelayedMySQLDriverModesPubSub,
			Payloads:  jsonColumnPayloads,
		},
	}

	for _, db := range dbs {
		for _, bind := range bindJSONAsTextOptions {
			if bind.RequiresKnownParameterTypes && !db.KnowsParameterTypes {
				continue
			}

			for _, pubSub := range pubSubs {
				t.Run(db.Name+"/"+bind.Name+"/"+pubSub.Name, func(t *testing.T) {
					t.Parallel()

					pub, sub := pubSub.NewPubSub(t, db.DB, bind.BindJSONAsText)

					testPublishAndReceiveAll(t, pub, sub, pubSub.Payloads)
				})
			}
		}
	}
}

// Stands for schemas customized before GeneratePayloadType existed, or tables created by migrations:
// a BYTEA payload column that isn't declared with GeneratePayloadType.
type byteaPayloadSchema struct {
	sql.DefaultPostgreSQLSchema
}

func (s byteaPayloadSchema) SchemaInitializingQueries(params sql.SchemaInitializingQueriesParams) ([]sql.Query, error) {
	schema := s.DefaultPostgreSQLSchema
	schema.GeneratePayloadType = payloadType("BYTEA")

	return schema.SchemaInitializingQueries(params)
}

// Without BindJSONAsText, the payload is bound as []byte whatever the payload column type is said to be,
// so a BYTEA column that isn't declared keeps working.
func TestPostgreSQLUndeclaredByteaPayload_round_trip(t *testing.T) {
	t.Parallel()

	dbs := []driverModesDB{
		{Name: "lib_pq", DB: newPostgreSQL(t), KnowsParameterTypes: true},
		{Name: "pgx_stdlib_default", DB: newPgxPostgreSQL(t), KnowsParameterTypes: true},
		{Name: "pgx_default", DB: newPgx(t), KnowsParameterTypes: true},
	}

	payloads := []message.Payload{
		message.Payload(`{"path": "C:\\dir"}`),
		message.Payload(`\101BC`),
		{0x00, 0x01, 0xff, 0xfe, 0x80, 'a', 0xc3, 0x28, 0x00},
		nil,
	}

	for _, db := range dbs {
		t.Run(db.Name, func(t *testing.T) {
			t.Parallel()

			schemaAdapter := byteaPayloadSchema{
				DefaultPostgreSQLSchema: sql.DefaultPostgreSQLSchema{
					InitializeSchemaLock: rand.Intn(1000000),
				},
			}
			pub, sub := newDriverModesPubSub(t, db.DB, schemaAdapter, sql.DefaultPostgreSQLOffsetsAdapter{})

			topic := newDriverModesTopic()
			messages, err := sub.Subscribe(t.Context(), topic)
			require.NoError(t, err)

			// One Publish call per message, so a payload that is rejected doesn't hide what happens to the others.
			var published []*message.Message
			for _, payload := range payloads {
				msg := newDriverModesMessage(payload)
				if assert.NoError(t, pub.Publish(topic, msg), "cannot publish payload %q", payload) {
					published = append(published, msg)
				}
			}

			received := receiveDriverModesMessages(t, messages, len(published))

			for _, msg := range published {
				stored := storedPayload(t, db.DB, schemaAdapter.MessagesTable(topic), msg.UUID)
				assert.Equal(t, msg.Payload, stored, "stored payload of message %s", msg.UUID)

				assertDriverModesMessageReceived(t, msg, received)
			}
		})
	}
}

// Documents why BindJSONAsText requires the payload column type to match the real column: pgx, in every query
// exec mode, sends text bound to a BYTEA column as bytea input, so payloads are silently altered or rejected.
// lib/pq stores them unchanged.
func TestPostgreSQLBindJSONAsText_bytea_column_said_to_be_json(t *testing.T) {
	t.Parallel()

	dbs := []driverModesDB{
		{Name: "pgx_stdlib_default", DB: newPgxPostgreSQL(t)},
		{Name: "pgx_default", DB: newPgx(t)},
		{Name: "pgx_exec", DB: newPgxWithQueryExecMode(t, "exec")},
		{Name: "pgx_simple_protocol", DB: newPgxWithQueryExecMode(t, "simple_protocol")},
	}

	payloadTypes := []struct {
		Name                string
		GeneratePayloadType func(topic string) string
	}{
		{Name: "not_declared", GeneratePayloadType: nil},
		{Name: "declared_json", GeneratePayloadType: payloadType("JSON")},
	}

	for _, db := range dbs {
		for _, pt := range payloadTypes {
			t.Run(db.Name+"/"+pt.Name, func(t *testing.T) {
				t.Parallel()

				schemaAdapter := byteaPayloadSchema{
					DefaultPostgreSQLSchema: sql.DefaultPostgreSQLSchema{
						GeneratePayloadType:  pt.GeneratePayloadType,
						BindJSONAsText:       true,
						InitializeSchemaLock: rand.Intn(1000000),
					},
				}
				pub, _ := newDriverModesPubSub(t, db.DB, schemaAdapter, sql.DefaultPostgreSQLOffsetsAdapter{})
				topic := newDriverModesTopic()
				table := schemaAdapter.MessagesTable(topic)

				altered := []struct {
					Published message.Payload
					Stored    message.Payload
				}{
					{Published: message.Payload(`{"path": "C:\\dir"}`), Stored: message.Payload(`{"path": "C:\dir"}`)},
					{Published: message.Payload(`\101BC`), Stored: message.Payload(`ABC`)},
				}
				for _, payload := range altered {
					msg := newDriverModesMessage(payload.Published)
					require.NoError(t, pub.Publish(topic, msg))

					assert.Equal(t, payload.Stored, storedPayload(t, db.DB, table, msg.UUID))
				}

				err := pub.Publish(topic, newDriverModesMessage(message.Payload{'a', 0x00, 'b'}))
				assert.Error(t, err, "a payload with a NUL byte should be rejected")
			})
		}
	}
}

func newDriverModesPubSub(
	t *testing.T,
	db sql.Beginner,
	schemaAdapter sql.SchemaAdapter,
	offsetsAdapter sql.OffsetsAdapter,
) (message.Publisher, message.Subscriber) {
	pub, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		PollInterval:     10 * time.Millisecond,
		ResendInterval:   10 * time.Millisecond,
		SchemaAdapter:    schemaAdapter,
		OffsetsAdapter:   offsetsAdapter,
		InitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, sub.Close())
		assert.NoError(t, pub.Close())
	})

	return pub, sub
}

func newDelayedMySQLDriverModesPubSub(t *testing.T, db sql.Beginner, bindJSONAsText bool) (message.Publisher, message.Subscriber) {
	pub, err := sql.NewDelayedMySQLPublisher(db, sql.DelayedMySQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			AllowNoDelay: true,
		},
		BindJSONAsText: bindJSONAsText,
		Logger:         logger,
	})
	require.NoError(t, err)

	sub, err := sql.NewDelayedMySQLSubscriber(db, sql.DelayedMySQLSubscriberConfig{
		OverrideSubscriberConfig: func(config *sql.SubscriberConfig) error {
			config.PollInterval = 10 * time.Millisecond
			return nil
		},
		DeleteOnAck:  true,
		AllowNoDelay: true,
		Logger:       logger,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, sub.Close())
		assert.NoError(t, pub.Close())
	})

	return pub, sub
}

func newDriverModesTopic() string {
	return "driver_modes_" + watermill.NewShortUUID()
}

func newDriverModesMessage(payload message.Payload) *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), payload)
	msg.Metadata.Set("text", "zażółć 🎉")
	msg.Metadata.Set("quote", `it's "q" \ x`)

	return msg
}

func testPublishAndReceiveAll(t *testing.T, pub message.Publisher, sub message.Subscriber, payloads []message.Payload) {
	t.Helper()

	topic := newDriverModesTopic()

	messages, err := sub.Subscribe(t.Context(), topic)
	require.NoError(t, err)

	var published []*message.Message
	for _, payload := range payloads {
		published = append(published, newDriverModesMessage(payload))
	}

	err = pub.Publish(topic, published...)
	require.NoError(t, err, "cannot publish messages")

	received := receiveDriverModesMessages(t, messages, len(published))

	for _, msg := range published {
		assertDriverModesMessageReceived(t, msg, received)
	}
}

func receiveDriverModesMessages(t *testing.T, messages <-chan *message.Message, count int) map[string]*message.Message {
	t.Helper()

	received := map[string]*message.Message{}
	for range count {
		select {
		case msg := <-messages:
			received[msg.UUID] = msg
			msg.Ack()
		case <-time.After(30 * time.Second):
			require.Fail(t, "timed out waiting for messages", "received %d of %d", len(received), count)
		}
	}

	return received
}

func assertDriverModesMessageReceived(t *testing.T, published *message.Message, received map[string]*message.Message) {
	t.Helper()

	got, ok := received[published.UUID]
	if !assert.True(t, ok, "message %s not received", published.UUID) {
		return
	}

	// message.Payload is not []byte, so assert.Equal compares with reflect.DeepEqual
	// and tells a nil payload (NULL) apart from an empty one.
	assert.Equal(t, published.Payload, got.Payload, "payload of message %s", published.UUID)
	assert.Equal(t, published.Metadata, got.Metadata, "metadata of message %s", published.UUID)
}

func storedPayload(t *testing.T, db sql.Beginner, table string, uuid string) message.Payload {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT payload FROM "+table+" WHERE uuid = $1", uuid)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	require.True(t, rows.Next(), "message %s not stored", uuid)

	var payload []byte
	require.NoError(t, rows.Scan(&payload))

	return payload
}

func postgreSQLTestConnString() string {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	return fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
}

func newPgxStdlibWithQueryExecMode(t *testing.T, mode string) sql.Beginner {
	conf, err := pgx.ParseConfig(postgreSQLTestConnString() + "&default_query_exec_mode=" + mode)
	require.NoError(t, err)

	db := stdlib.OpenDB(*conf)
	t.Cleanup(func() { _ = db.Close() })

	err = db.Ping()
	require.NoError(t, err)

	return sql.BeginnerFromStdSQL(db)
}

func newPgxWithQueryExecMode(t *testing.T, mode string) sql.Beginner {
	conf, err := pgxpool.ParseConfig(postgreSQLTestConnString() + "&default_query_exec_mode=" + mode)
	require.NoError(t, err)

	db, err := pgxpool.NewWithConfig(context.Background(), conf)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	err = db.Ping(context.Background())
	require.NoError(t, err)

	return sql.BeginnerFromPgx(db)
}

func newMySQLWithInterpolateParams(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_MYSQL_HOST")
	if addr == "" {
		addr = "localhost"
	}
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = addr
	conf.DBName = "watermill"
	conf.InterpolateParams = true

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	err = db.Ping()
	require.NoError(t, err)

	return sql.BeginnerFromStdSQL(db)
}

func payloadType(columnType string) func(topic string) string {
	return func(topic string) string {
		return columnType
	}
}

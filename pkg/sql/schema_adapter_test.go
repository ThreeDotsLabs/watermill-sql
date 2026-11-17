package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

// TestDefaultMySQLSchema checks if the SQL schema defined in DefaultMySQLSchema is correctly executed
// and if message marshaling works as intended.
func TestDefaultMySQLSchema(t *testing.T) {
	db := newMySQL(t)

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		SchemaAdapter:    sql.DefaultMySQLSchema{},
		OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
		InitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	testOneMessage(t, publisher, subscriber)
}

func TestDefaultMySQLSchema_implicit_commit_warning(t *testing.T) {
	db := newMySQL(t)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	schemaAdapter := sql.DefaultMySQLSchema{}
	_, err = sql.NewPublisher(tx, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: true,
	}, logger)
	require.Error(t, err, "used auto schema initializing without a separate db handle for the adapter, "+
		"expected error from publisher constructor")
}

func TestDefaultMySQLSchema_implicit_commit(t *testing.T) {
	db := newMySQL(t)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	schemaAdapter := sql.DefaultMySQLSchema{}
	_, err = sql.NewPublisher(tx, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: true,
	}, logger)
	require.Error(t, err, "expecting error with AutoInitializeSchema and db handle which is a tx")
}

// TestDefaultPostgreSQLSchema checks if the SQL schema defined in DefaultPostgreSQLSchema is correctly executed
// and if message marshaling works as intended.
func TestDefaultPostgreSQLSchema(t *testing.T) {
	db := newPostgreSQL(t)

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		SchemaAdapter:    sql.DefaultPostgreSQLSchema{},
		OffsetsAdapter:   sql.DefaultPostgreSQLOffsetsAdapter{},
		InitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	testOneMessage(t, publisher, subscriber)
}

type fakeMessageRow struct {
	values []any
}

func (r fakeMessageRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return fmt.Errorf("expected %d scan destinations, got %d", len(r.values), len(dest))
	}

	for i, d := range dest {
		switch d := d.(type) {
		case *int64:
			*d = r.values[i].(int64)
		case *[]byte:
			*d = r.values[i].([]byte)
		case stdSQL.Scanner:
			if err := d.Scan(r.values[i]); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported scan destination %T", d)
		}
	}

	return nil
}

type unmarshalMessageTestCase struct {
	Name          string
	SchemaAdapter sql.SchemaAdapter
	Row           fakeMessageRow
}

func unmarshalMessageTestCases(metadata []byte) []unmarshalMessageTestCase {
	offset := int64(1)
	transactionID := int64(1)
	uuid := []byte(watermill.NewUUID())
	payload := []byte("{}")

	return []unmarshalMessageTestCase{
		{
			Name:          "mysql",
			SchemaAdapter: sql.DefaultMySQLSchema{},
			Row:           fakeMessageRow{values: []any{offset, uuid, payload, metadata}},
		},
		{
			Name:          "mysql_queue",
			SchemaAdapter: sql.MySQLQueueSchema{},
			Row:           fakeMessageRow{values: []any{offset, uuid, payload, metadata}},
		},
		{
			Name:          "postgresql",
			SchemaAdapter: sql.DefaultPostgreSQLSchema{},
			Row:           fakeMessageRow{values: []any{offset, transactionID, uuid, payload, metadata}},
		},
		{
			Name:          "postgresql_queue",
			SchemaAdapter: sql.PostgreSQLQueueSchema{},
			Row:           fakeMessageRow{values: []any{offset, uuid, payload, metadata}},
		},
	}
}

// TestUnmarshalMessage_metadata_stored_as_JSON_null checks that a message published with nil Metadata
// (stored as JSON null) is received with Metadata that can be modified.
func TestUnmarshalMessage_metadata_stored_as_JSON_null(t *testing.T) {
	t.Parallel()

	for _, tc := range unmarshalMessageTestCases([]byte("null")) {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			row, err := tc.SchemaAdapter.UnmarshalMessage(sql.UnmarshalMessageParams{Row: tc.Row})
			require.NoError(t, err)

			require.NotNil(t, row.Msg.Metadata)
			assert.NotPanics(t, func() { row.Msg.Metadata.Set("key", "value") })
		})
	}
}

func TestUnmarshalMessage_metadata_stored_as_JSON_object(t *testing.T) {
	t.Parallel()

	for _, tc := range unmarshalMessageTestCases([]byte(`{"key":"value"}`)) {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			row, err := tc.SchemaAdapter.UnmarshalMessage(sql.UnmarshalMessageParams{Row: tc.Row})
			require.NoError(t, err)

			assert.Equal(t, message.Metadata{"key": "value"}, row.Msg.Metadata)
		})
	}
}

// TestPublishSubscribe_message_with_nil_metadata checks that a consumer can set metadata
// on a message which was published without metadata.
func TestPublishSubscribe_message_with_nil_metadata(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
	}{
		{
			Name:        "mysql",
			Constructor: createMySQLPubSub,
		},
		{
			Name:        "postgresql",
			Constructor: createPostgreSQLPubSub,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			pub, sub := tc.Constructor(t)
			topic := "topic_" + watermill.NewUUID()

			err := sub.(message.SubscribeInitializer).SubscribeInitialize(topic)
			require.NoError(t, err)

			// Built without message.NewMessage, so Metadata is nil.
			msg := &message.Message{
				UUID:    watermill.NewUUID(),
				Payload: []byte("{}"),
			}
			err = pub.Publish(topic, msg)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			messages, err := sub.Subscribe(ctx, topic)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, sub.Close())
			}()

			select {
			case received := <-messages:
				require.Equal(t, msg.UUID, received.UUID)
				require.NotPanics(t, func() { received.Metadata.Set("key", "value") })
				assert.Equal(t, "value", received.Metadata.Get("key"))
				require.True(t, received.Ack())
			case <-time.After(30 * time.Second):
				t.Fatal("message not received")
			}
		})
	}
}

func testOneMessage(t *testing.T, publisher message.Publisher, subscriber message.Subscriber) {
	topic := "test_" + watermill.NewULID()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewULID(), []byte(`{"json": "field"}`))
	err = publisher.Publish(topic, msg)
	require.NoError(t, err)

	select {
	case received := <-messages:
		require.Equal(t, msg.UUID, received.UUID)
		require.Equal(t, msg.Payload, received.Payload)
		received.Ack()
	case <-time.After(time.Second * 5):
		t.Error("Didn't receive any messages")
	}
}

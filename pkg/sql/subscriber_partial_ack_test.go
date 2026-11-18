package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Guards against the ack query of a partially acked batch (a message not acked before AckDeadline,
// or the subscriber closed while one is in flight) covering the whole batch, which lost the rest of it.
// The fake database lets the test check the ack query's arguments directly.
func TestSubscriber_queue_partially_acked_batch(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name           string
		SchemaAdapter  sql.SchemaAdapter
		OffsetsAdapter sql.OffsetsAdapter
	}{
		{
			Name:           "postgresql_delete_on_ack",
			SchemaAdapter:  sql.PostgreSQLQueueSchema{},
			OffsetsAdapter: sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true},
		},
		{
			Name:           "postgresql_mark_acked",
			SchemaAdapter:  sql.PostgreSQLQueueSchema{},
			OffsetsAdapter: sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: false},
		},
		{
			Name:           "mysql_delete_on_ack",
			SchemaAdapter:  sql.MySQLQueueSchema{},
			OffsetsAdapter: sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true},
		},
		{
			Name:           "mysql_mark_acked",
			SchemaAdapter:  sql.MySQLQueueSchema{},
			OffsetsAdapter: sql.MySQLQueueOffsetsAdapter{DeleteOnAck: false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			t.Run("ack_deadline_exceeded", func(t *testing.T) {
				t.Parallel()

				db := &fakeQueueDB{offsets: []int64{11, 12, 13, 14}}
				sub := newQueueSubscriber(t, db, tc.SchemaAdapter, tc.OffsetsAdapter, 200*time.Millisecond)

				messages, err := sub.Subscribe(context.Background(), "topic")
				require.NoError(t, err)

				go func() {
					for msg := range messages {
						// The other messages are never acked, so the ack deadline of message 12 expires.
						if msg.UUID == "11" {
							msg.Ack()
						}
					}
				}()

				require.Eventually(t, func() bool {
					return len(db.Executed()) > 0
				}, 30*time.Second, 5*time.Millisecond, "ack query was not executed")
				require.NoError(t, sub.Close())

				assert.Equal(t, []int64{11}, ackedOffsets(t, db.Executed()))
			})

			t.Run("subscriber_closed", func(t *testing.T) {
				t.Parallel()

				db := &fakeQueueDB{offsets: []int64{11, 12, 13, 14}}
				sub := newQueueSubscriber(t, db, tc.SchemaAdapter, tc.OffsetsAdapter, 30*time.Second)

				messages, err := sub.Subscribe(context.Background(), "topic")
				require.NoError(t, err)

				first := receiveMessage(t, messages)
				require.Equal(t, "11", first.UUID)
				first.Ack()

				second := receiveMessage(t, messages)
				require.Equal(t, "12", second.UUID)

				require.NoError(t, sub.Close())

				assert.Equal(t, []int64{11}, ackedOffsets(t, db.Executed()))
			})
		})
	}
}

// TestPostgreSQLQueue_partially_acked_batch checks that messages not acked in a partially acked batch
// are still in the PostgreSQL queue and are delivered again.
func TestPostgreSQLQueue_partially_acked_batch(t *testing.T) {
	t.Parallel()

	testQueuePartiallyAckedBatch(
		t,
		newPostgreSQL(t),
		sql.PostgreSQLQueueSchema{},
		func(deleteOnAck bool) sql.OffsetsAdapter {
			return sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: deleteOnAck}
		},
	)
}

// TestMySQLQueue_partially_acked_batch checks that messages not acked in a partially acked batch
// are still in the MySQL queue and are delivered again.
func TestMySQLQueue_partially_acked_batch(t *testing.T) {
	t.Parallel()

	testQueuePartiallyAckedBatch(
		t,
		newMySQL(t),
		sql.MySQLQueueSchema{},
		func(deleteOnAck bool) sql.OffsetsAdapter {
			return sql.MySQLQueueOffsetsAdapter{DeleteOnAck: deleteOnAck}
		},
	)
}

type queueSchemaAdapter interface {
	sql.SchemaAdapter
	MessagesTable(topic string) string
}

func testQueuePartiallyAckedBatch(
	t *testing.T,
	db sql.Beginner,
	schemaAdapter queueSchemaAdapter,
	newOffsetsAdapter func(deleteOnAck bool) sql.OffsetsAdapter,
) {
	t.Helper()

	publishedUUIDs := []string{"1", "2", "3", "4", "5"}
	notAckedUUIDs := publishedUUIDs[1:]

	for _, deleteOnAck := range []bool{true, false} {
		offsetsAdapter := newOffsetsAdapter(deleteOnAck)

		name := "mark_acked"
		if deleteOnAck {
			name = "delete_on_ack"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Run("ack_deadline_exceeded", func(t *testing.T) {
				t.Parallel()

				topic := watermill.NewUUID()
				table := schemaAdapter.MessagesTable(topic)
				publishQueueMessages(t, db, schemaAdapter, topic, publishedUUIDs)

				sub := newQueueSubscriber(t, db, schemaAdapter, offsetsAdapter, 200*time.Millisecond)

				messages, err := sub.Subscribe(context.Background(), topic)
				require.NoError(t, err)

				go func() {
					for msg := range messages {
						// The other messages are never acked, so the ack deadline of message 2 expires.
						if msg.UUID == "1" {
							msg.Ack()
						}
					}
				}()

				require.EventuallyWithT(t, func(t *assert.CollectT) {
					assert.Less(t, countNotAckedQueueMessages(t, db, table), len(publishedUUIDs))
				}, 30*time.Second, 10*time.Millisecond, "first message was not acked")
				require.NoError(t, sub.Close())

				require.Equal(t, len(notAckedUUIDs), countNotAckedQueueMessages(t, db, table))
				requireQueueMessagesReceived(t, db, schemaAdapter, offsetsAdapter, topic, notAckedUUIDs)
			})

			t.Run("subscriber_closed", func(t *testing.T) {
				t.Parallel()

				topic := watermill.NewUUID()
				table := schemaAdapter.MessagesTable(topic)
				publishQueueMessages(t, db, schemaAdapter, topic, publishedUUIDs)

				sub := newQueueSubscriber(t, db, schemaAdapter, offsetsAdapter, 30*time.Second)

				messages, err := sub.Subscribe(context.Background(), topic)
				require.NoError(t, err)

				first := receiveMessage(t, messages)
				require.Equal(t, "1", first.UUID)
				first.Ack()

				second := receiveMessage(t, messages)
				require.Equal(t, "2", second.UUID)

				require.NoError(t, sub.Close())

				require.Equal(t, len(notAckedUUIDs), countNotAckedQueueMessages(t, db, table))
				requireQueueMessagesReceived(t, db, schemaAdapter, offsetsAdapter, topic, notAckedUUIDs)
			})
		})
	}
}

func publishQueueMessages(
	t *testing.T,
	db sql.Beginner,
	schemaAdapter sql.SchemaAdapter,
	topic string,
	uuids []string,
) {
	t.Helper()

	pub, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	var msgs []*message.Message
	for _, uuid := range uuids {
		msgs = append(msgs, message.NewMessage(uuid, []byte("{}")))
	}

	// All messages are published in one query, so they are fetched by the subscriber in one batch.
	err = pub.Publish(topic, msgs...)
	require.NoError(t, err)
}

func newQueueSubscriber(
	t *testing.T,
	db sql.Beginner,
	schemaAdapter sql.SchemaAdapter,
	offsetsAdapter sql.OffsetsAdapter,
	ackDeadline time.Duration,
) *sql.Subscriber {
	t.Helper()

	sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		AckDeadline:    &ackDeadline,
		PollInterval:   10 * time.Millisecond,
		SchemaAdapter:  schemaAdapter,
		OffsetsAdapter: offsetsAdapter,
	}, logger)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, sub.Close())
	})

	return sub
}

func receiveMessage(t *testing.T, messages <-chan *message.Message) *message.Message {
	t.Helper()

	select {
	case msg, ok := <-messages:
		require.True(t, ok, "messages channel closed")
		return msg
	case <-time.After(30 * time.Second):
		t.Fatal("no message received")
		return nil
	}
}

func requireQueueMessagesReceived(
	t *testing.T,
	db sql.Beginner,
	schemaAdapter sql.SchemaAdapter,
	offsetsAdapter sql.OffsetsAdapter,
	topic string,
	expectedUUIDs []string,
) {
	t.Helper()

	sub := newQueueSubscriber(t, db, schemaAdapter, offsetsAdapter, 30*time.Second)

	messages, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	var receivedUUIDs []string
	for range expectedUUIDs {
		msg := receiveMessage(t, messages)
		receivedUUIDs = append(receivedUUIDs, msg.UUID)
		msg.Ack()
	}

	require.NoError(t, sub.Close())

	assert.Equal(t, expectedUUIDs, receivedUUIDs)
}

func countNotAckedQueueMessages(t require.TestingT, db sql.Beginner, table string) int {
	rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM "+table+" WHERE acked = false")
	require.NoError(t, err)

	require.True(t, rows.Next(), "count query returned no rows")

	var count int
	require.NoError(t, rows.Scan(&count))
	require.NoError(t, rows.Close())

	return count
}

func ackedOffsets(t *testing.T, executed []sql.Query) []int64 {
	t.Helper()

	require.Len(t, executed, 1, "expected exactly one ack query")

	var offsets []int64
	for _, arg := range executed[0].Args {
		switch arg := arg.(type) {
		case *pq.Int64Array:
			// PostgreSQLQueueOffsetsAdapter passes all offsets as a single array argument.
			offsets = append(offsets, *arg...)
		case int64:
			// MySQLQueueOffsetsAdapter passes each offset as a separate argument.
			offsets = append(offsets, arg)
		default:
			t.Fatalf("unexpected ack query argument type %T", arg)
		}
	}

	return offsets
}

// fakeQueueDB serves no rows after the first executed statement (the ack query, as the queue adapters
// have no consumed query), so the subscriber doesn't redeliver the batch and runs exactly one ack query.
type fakeQueueDB struct {
	offsets []int64

	mu       sync.Mutex
	executed []sql.Query
}

func (db *fakeQueueDB) BeginTx(context.Context, *stdSQL.TxOptions) (sql.Tx, error) {
	return fakeQueueTx{db}, nil
}

func (db *fakeQueueDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.executed = append(db.executed, sql.Query{Query: query, Args: args})

	return fakeResult{}, nil
}

func (db *fakeQueueDB) QueryContext(context.Context, string, ...any) (sql.Rows, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.executed) > 0 {
		return &fakeQueueRows{}, nil
	}

	return &fakeQueueRows{offsets: db.offsets}, nil
}

func (db *fakeQueueDB) Executed() []sql.Query {
	db.mu.Lock()
	defer db.mu.Unlock()

	return append([]sql.Query(nil), db.executed...)
}

type fakeQueueTx struct {
	*fakeQueueDB
}

func (fakeQueueTx) Commit() error {
	return nil
}

func (fakeQueueTx) Rollback() error {
	return nil
}

type fakeResult struct{}

func (fakeResult) RowsAffected() (int64, error) {
	return 0, nil
}

type fakeQueueRows struct {
	offsets []int64
	next    int
}

func (r *fakeQueueRows) Next() bool {
	if r.next >= len(r.offsets) {
		return false
	}

	r.next++

	return true
}

func (r *fakeQueueRows) Scan(dest ...any) error {
	if len(dest) != 4 {
		return fmt.Errorf("expected 4 scan destinations, got %d", len(dest))
	}

	offset, okOffset := dest[0].(*int64)
	uuid, okUUID := dest[1].(*[]byte)
	payload, okPayload := dest[2].(*[]byte)
	metadata, okMetadata := dest[3].(*[]byte)
	if !okOffset || !okUUID || !okPayload || !okMetadata {
		return fmt.Errorf("unexpected scan destination types: %T, %T, %T, %T", dest...)
	}

	*offset = r.offsets[r.next-1]
	*uuid = []byte(fmt.Sprint(*offset))
	*payload = []byte("{}")
	*metadata = nil

	return nil
}

func (r *fakeQueueRows) Close() error {
	return nil
}

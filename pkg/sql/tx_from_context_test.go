package sql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

type txTestBackend struct {
	Name           string
	DB             func(t *testing.T) sql.Beginner
	SchemaAdapter  sql.SchemaAdapter
	OffsetsAdapter sql.OffsetsAdapter

	Quote        func(identifier string) string
	Placeholders [2]string

	KillConnection func(ctx context.Context, tx sql.Tx)

	// database/sql serializes access to the connection; a pgx connection must not be used concurrently.
	TxSafeForConcurrentUse bool
}

func txTestBackends() []txTestBackend {
	quotePostgres := func(identifier string) string { return `"` + identifier + `"` }
	quoteMySQL := func(identifier string) string { return "`" + identifier + "`" }

	// The statements fail, because they close their own connection.
	killPostgresConnection := func(ctx context.Context, tx sql.Tx) {
		_, _ = tx.ExecContext(ctx, "SELECT pg_terminate_backend(pg_backend_pid())")
	}
	killMySQLConnection := func(ctx context.Context, tx sql.Tx) {
		_, _ = tx.ExecContext(ctx, "KILL CONNECTION_ID()")
	}

	return []txTestBackend{
		{
			Name:                   "postgresql",
			DB:                     newPostgreSQL,
			SchemaAdapter:          newPostgresSchemaAdapter(0),
			OffsetsAdapter:         newPostgresOffsetsAdapter(),
			Quote:                  quotePostgres,
			Placeholders:           [2]string{"$1", "$2"},
			KillConnection:         killPostgresConnection,
			TxSafeForConcurrentUse: true,
		},
		{
			Name:                   "pgx_stdlib",
			DB:                     newPgxPostgreSQL,
			SchemaAdapter:          newPostgresSchemaAdapter(0),
			OffsetsAdapter:         newPostgresOffsetsAdapter(),
			Quote:                  quotePostgres,
			Placeholders:           [2]string{"$1", "$2"},
			KillConnection:         killPostgresConnection,
			TxSafeForConcurrentUse: true,
		},
		{
			Name:           "pgx",
			DB:             newPgx,
			SchemaAdapter:  newPostgresSchemaAdapter(0),
			OffsetsAdapter: newPostgresOffsetsAdapter(),
			Quote:          quotePostgres,
			Placeholders:   [2]string{"$1", "$2"},
			KillConnection: killPostgresConnection,
		},
		{
			Name:                   "mysql",
			DB:                     newMySQL,
			SchemaAdapter:          newMySQLSchemaAdapter(0),
			OffsetsAdapter:         newMySQLOffsetsAdapter(),
			Quote:                  quoteMySQL,
			Placeholders:           [2]string{"?", "?"},
			KillConnection:         killMySQLConnection,
			TxSafeForConcurrentUse: true,
		},
	}
}

type txTestEnv struct {
	Backend     txTestBackend
	DB          sql.Beginner
	Topic       string
	Publisher   message.Publisher
	SideEffects sideEffects
}

func newTxTestEnv(t *testing.T, backend txTestBackend) txTestEnv {
	t.Helper()

	db := backend.DB(t)

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{SchemaAdapter: backend.SchemaAdapter}, logger)
	require.NoError(t, err)

	return txTestEnv{
		Backend:     backend,
		DB:          db,
		Topic:       "topic_" + watermill.NewUUID(),
		Publisher:   publisher,
		SideEffects: newSideEffects(t, backend, db),
	}
}

func (e txTestEnv) newSubscriber(t *testing.T, ackDeadline time.Duration) *sql.Subscriber {
	t.Helper()

	sub, err := sql.NewSubscriber(
		e.DB,
		sql.SubscriberConfig{
			ConsumerGroup:  "test",
			AckDeadline:    &ackDeadline,
			PollInterval:   10 * time.Millisecond,
			ResendInterval: 10 * time.Millisecond,
			RetryInterval:  10 * time.Millisecond,
			SchemaAdapter:  e.Backend.SchemaAdapter,
			OffsetsAdapter: e.Backend.OffsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, sub.Close())
	})

	require.NoError(t, sub.SubscribeInitialize(e.Topic))

	return sub
}

func (e txTestEnv) subscribe(t *testing.T, sub *sql.Subscriber) <-chan *message.Message {
	t.Helper()

	messages, err := sub.Subscribe(context.Background(), e.Topic)
	require.NoError(t, err)

	return messages
}

func (e txTestEnv) publish(t *testing.T, count int) []*message.Message {
	t.Helper()

	var msgs []*message.Message
	for range count {
		msgs = append(msgs, message.NewMessage(watermill.NewUUID(), nil))
	}

	require.NoError(t, e.Publisher.Publish(e.Topic, msgs...))

	return msgs
}

func (e txTestEnv) requireNoRedelivery(t *testing.T, messages <-chan *message.Message) {
	t.Helper()

	next := e.publish(t, 1)[0]

	received := receiveMessage(t, messages)
	received.Ack()
	require.Equal(t, next.UUID, received.UUID, "an already processed message was delivered again")
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

func receiveMessageWithUUID(t *testing.T, messages <-chan *message.Message, uuid string) *message.Message {
	t.Helper()

	msg := receiveMessage(t, messages)
	require.Equal(t, uuid, msg.UUID, "unexpected message received")

	return msg
}

type sideEffects struct {
	db    sql.Beginner
	table string

	insertQuery string
	countQuery  string
}

func newSideEffects(t *testing.T, backend txTestBackend, db sql.Beginner) sideEffects {
	t.Helper()

	table := backend.Quote("test_side_effects_" + watermill.NewUUID())

	_, err := db.ExecContext(
		context.Background(),
		"CREATE TABLE "+table+" (id VARCHAR(36) NOT NULL PRIMARY KEY, message_uuid VARCHAR(36) NOT NULL)",
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP TABLE "+table)
		assert.NoError(t, err)
	})

	return sideEffects{
		db:    db,
		table: table,
		insertQuery: "INSERT INTO " + table + " (id, message_uuid) VALUES (" +
			backend.Placeholders[0] + ", " + backend.Placeholders[1] + ")",
		countQuery: "SELECT COUNT(*) FROM " + table + " WHERE message_uuid = " + backend.Placeholders[0],
	}
}

func (s sideEffects) insert(msg *message.Message) error {
	return s.insertWithID(msg, watermill.NewUUID())
}

func (s sideEffects) insertWithID(msg *message.Message, id string) error {
	tx, ok := sql.TxFromContext(msg.Context())
	if !ok {
		return errors.New("no transaction in the message context")
	}

	_, err := tx.ExecContext(msg.Context(), s.insertQuery, id, msg.UUID)

	return err
}

func (s sideEffects) insertCommitted(t *testing.T, id string, messageUUID string) {
	t.Helper()

	_, err := s.db.ExecContext(context.Background(), s.insertQuery, id, messageUUID)
	require.NoError(t, err)
}

func (s sideEffects) count(t *testing.T, messageUUID string) int {
	t.Helper()

	count, err := s.queryCount(messageUUID)
	require.NoError(t, err)

	return count
}

func (s sideEffects) waitForCommitted(t *testing.T, messageUUID string) int {
	t.Helper()

	require.Eventually(t, func() bool {
		count, err := s.queryCount(messageUUID)
		return err == nil && count > 0
	}, 30*time.Second, 10*time.Millisecond, "side effects of message %s were not committed", messageUUID)

	return s.count(t, messageUUID)
}

func (s sideEffects) queryCount(messageUUID string) (int, error) {
	rows, err := s.db.QueryContext(context.Background(), s.countQuery, messageUUID)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		return 0, errors.New("count query returned no rows")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

func TestTxFromContext_nack_rolls_back_handler_changes(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)
			sub := env.newSubscriber(t, 30*time.Second)
			msg := env.publish(t, 1)[0]
			messages := env.subscribe(t, sub)

			first := receiveMessageWithUUID(t, messages, msg.UUID)
			require.NoError(t, env.SideEffects.insert(first))
			first.Nack()

			second := receiveMessageWithUUID(t, messages, msg.UUID)
			require.NoError(t, env.SideEffects.insert(second))
			second.Ack()

			assert.Equal(t, 1, env.SideEffects.waitForCommitted(t, msg.UUID), "only the acked delivery should be committed")
			env.requireNoRedelivery(t, messages)
		})
	}
}

func TestTxFromContext_ack_deadline_rolls_back_handler_changes(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		if !backend.TxSafeForConcurrentUse {
			// When the handler doesn't ack, nothing orders its last statement before the subscriber's next statement
			// on the same connection, and the race detector reports it.
			continue
		}

		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)
			sub := env.newSubscriber(t, 500*time.Millisecond)
			msg := env.publish(t, 1)[0]
			messages := env.subscribe(t, sub)

			first := receiveMessageWithUUID(t, messages, msg.UUID)
			require.NoError(t, env.SideEffects.insert(first))
			// not acked: the ack deadline expires

			second := receiveMessageWithUUID(t, messages, msg.UUID)
			second.Ack()

			// No need to wait: the first delivery's transaction ended before the message was delivered again.
			assert.Equal(t, 0, env.SideEffects.count(t, msg.UUID), "changes of the delivery that was not acked should not be committed")
			env.requireNoRedelivery(t, messages)
		})
	}
}

func TestTxFromContext_close_rolls_back_handler_changes(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)
			sub := env.newSubscriber(t, 30*time.Second)
			msg := env.publish(t, 1)[0]
			messages := env.subscribe(t, sub)

			received := receiveMessageWithUUID(t, messages, msg.UUID)
			require.NoError(t, env.SideEffects.insert(received))

			// Close waits until the subscriber's transaction ends, so the count below is final.
			require.NoError(t, sub.Close())

			assert.Equal(t, 0, env.SideEffects.count(t, msg.UUID), "changes of the delivery that was not acked should not be committed")

			redelivered := receiveMessageWithUUID(t, env.subscribe(t, env.newSubscriber(t, 30*time.Second)), msg.UUID)
			redelivered.Ack()
		})
	}
}

func TestTxFromContext_failed_statement_does_not_break_redelivery(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)

			conflictingID := watermill.NewUUID()
			env.SideEffects.insertCommitted(t, conflictingID, "conflicting")

			// No ack deadline: in PostgreSQL, only a rollback after the nack can make
			// the transaction usable again (otherwise every statement fails with SQLSTATE 25P02).
			sub := env.newSubscriber(t, 0)
			msg := env.publish(t, 1)[0]
			messages := env.subscribe(t, sub)

			first := receiveMessageWithUUID(t, messages, msg.UUID)
			require.Error(t, env.SideEffects.insertWithID(first, conflictingID), "the insert should violate the primary key")
			first.Nack()

			second := receiveMessageWithUUID(t, messages, msg.UUID)
			require.NoError(t, env.SideEffects.insert(second), "the transaction should be usable after the failed delivery was nacked")
			second.Ack()

			assert.Equal(t, 1, env.SideEffects.waitForCommitted(t, msg.UUID))
			env.requireNoRedelivery(t, messages)
		})
	}
}

func TestTxFromContext_nack_in_batch_redelivers_whole_batch(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)
			sub := env.newSubscriber(t, 30*time.Second)
			// Published together, so they are read in one batch, within one transaction.
			msgs := env.publish(t, 3)
			messages := env.subscribe(t, sub)

			m1 := receiveMessageWithUUID(t, messages, msgs[0].UUID)
			require.NoError(t, env.SideEffects.insert(m1))
			m1.Ack()

			m2 := receiveMessageWithUUID(t, messages, msgs[1].UUID)
			require.NoError(t, env.SideEffects.insert(m2))
			m2.Nack()

			m1Again := receiveMessageWithUUID(t, messages, msgs[0].UUID)
			require.NoError(t, env.SideEffects.insert(m1Again))
			m1Again.Ack()

			m2Again := receiveMessageWithUUID(t, messages, msgs[1].UUID)
			require.NoError(t, env.SideEffects.insert(m2Again))
			m2Again.Ack()

			m3 := receiveMessageWithUUID(t, messages, msgs[2].UUID)
			require.NoError(t, env.SideEffects.insert(m3))
			m3.Ack()

			m1Tx, _ := sql.TxFromContext(m1.Context())
			m1AgainTx, _ := sql.TxFromContext(m1Again.Context())
			m3Tx, _ := sql.TxFromContext(m3.Context())
			require.NotSame(t, m1Tx, m1AgainTx, "the batch should be read again in a new transaction")
			require.Same(t, m1AgainTx, m3Tx, "the messages should be processed in one transaction")

			// All messages are committed in one transaction, so when one is visible, all are.
			assert.Equal(t, 1, env.SideEffects.waitForCommitted(t, msgs[2].UUID), "m3")
			assert.Equal(t, 1, env.SideEffects.count(t, msgs[0].UUID), "m1: the changes of the first delivery should be rolled back")
			assert.Equal(t, 1, env.SideEffects.count(t, msgs[1].UUID), "m2: only the acked delivery should be committed")
			env.requireNoRedelivery(t, messages)
		})
	}
}

func TestTxFromContext_nack_without_tx_does_not_redeliver_batch(t *testing.T) {
	t.Parallel()

	for _, backend := range txTestBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()

			env := newTxTestEnv(t, backend)
			sub := env.newSubscriber(t, 30*time.Second)
			msgs := env.publish(t, 3)
			messages := env.subscribe(t, sub)

			receiveMessageWithUUID(t, messages, msgs[0].UUID).Ack()
			receiveMessageWithUUID(t, messages, msgs[1].UUID).Nack()
			receiveMessageWithUUID(t, messages, msgs[1].UUID).Ack()
			receiveMessageWithUUID(t, messages, msgs[2].UUID).Ack()

			env.requireNoRedelivery(t, messages)
		})
	}
}

// When the transaction ends on the server side while database/sql or pgx still treat it as open
// (in MySQL, a deadlock rolls back the whole transaction), the message must be delivered again in a new transaction.
// Statements on the old transaction would run outside any transaction (autocommit), without the subscriber's locks.
func TestTxFromContext_ended_transaction_is_not_reused(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name           string
		EndTransaction func(t *testing.T, ctx context.Context, backend txTestBackend, tx sql.Tx)
	}{
		{
			// Leaves the transaction in the same state as a deadlock in MySQL.
			Name: "rollback",
			EndTransaction: func(t *testing.T, ctx context.Context, backend txTestBackend, tx sql.Tx) {
				_, err := tx.ExecContext(ctx, "ROLLBACK")
				require.NoError(t, err)
			},
		},
		{
			Name: "connection_killed",
			EndTransaction: func(t *testing.T, ctx context.Context, backend txTestBackend, tx sql.Tx) {
				backend.KillConnection(ctx, tx)
			},
		},
	}

	for _, backend := range txTestBackends() {
		for _, tc := range testCases {
			t.Run(backend.Name+"/"+tc.Name, func(t *testing.T) {
				t.Parallel()

				env := newTxTestEnv(t, backend)
				sub := env.newSubscriber(t, 30*time.Second)
				msg := env.publish(t, 1)[0]
				messages := env.subscribe(t, sub)

				first := receiveMessageWithUUID(t, messages, msg.UUID)
				firstTx, _ := sql.TxFromContext(first.Context())
				require.NoError(t, env.SideEffects.insert(first))
				tc.EndTransaction(t, first.Context(), backend, firstTx)
				first.Nack()

				second := receiveMessageWithUUID(t, messages, msg.UUID)
				secondTx, _ := sql.TxFromContext(second.Context())
				assert.NotSame(t, firstTx, secondTx, "the message should be delivered again in a new transaction")
				require.NoError(t, env.SideEffects.insert(second))
				assert.Equal(t, 0, env.SideEffects.count(t, msg.UUID), "the change should not be committed before the message is acked")
				second.Ack()

				assert.Equal(t, 1, env.SideEffects.waitForCommitted(t, msg.UUID))
				env.requireNoRedelivery(t, messages)
			})
		}
	}
}

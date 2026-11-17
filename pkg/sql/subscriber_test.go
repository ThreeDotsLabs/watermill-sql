package sql_test

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestSubscriber_rows_iteration_error_is_reported(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name    string
		Offsets []int64
	}{
		{
			Name:    "no_rows_before_error",
			Offsets: nil,
		},
		{
			Name:    "rows_before_error",
			Offsets: []int64{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			iterationErr := errors.New("rows iteration failed")
			db := fakeBeginner{
				newRows: func() *fakeRows {
					return &fakeRows{offsets: tc.Offsets, err: iterationErr}
				},
			}
			backoffManager := &recordingBackoffManager{}

			received := subscribeAndAckAll(t, newFakeDBSubscriber(t, db, backoffManager), "topic_"+watermill.NewUUID())

			firstCall := backoffManager.waitForFirstCall(t)
			assert.ErrorIs(t, firstCall.Err, iterationErr)
			assert.Empty(t, received.UUIDs(), "messages from an interrupted query should not be delivered")
		})
	}
}

func TestSubscriber_rows_close_error_is_reported(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("rows close failed")
	db := fakeBeginner{
		newRows: func() *fakeRows {
			return &fakeRows{closeErr: closeErr}
		},
	}
	backoffManager := &recordingBackoffManager{}

	subscribeAndAckAll(t, newFakeDBSubscriber(t, db, backoffManager), "topic_"+watermill.NewUUID())

	firstCall := backoffManager.waitForFirstCall(t)
	assert.ErrorIs(t, firstCall.Err, closeErr)
}

func TestSubscriber_select_failing_during_execution_is_reported(t *testing.T) {
	t.Parallel()

	dbConstructors := []struct {
		Name          string
		DbConstructor func(t *testing.T) sql.Beginner
	}{
		{
			Name:          "postgresql",
			DbConstructor: newPostgreSQL,
		},
		{
			Name:          "pgx_stdlib",
			DbConstructor: newPgxPostgreSQL,
		},
		{
			Name:          "pgx",
			DbConstructor: newPgx,
		},
	}

	testCases := []struct {
		Name          string
		FailOnMessage int
	}{
		{
			Name:          "fails_on_first_row",
			FailOnMessage: 0,
		},
		{
			Name:          "fails_after_first_row",
			FailOnMessage: 1,
		},
	}

	for _, dbConstructor := range dbConstructors {
		for _, tc := range testCases {
			t.Run(dbConstructor.Name+"/"+tc.Name, func(t *testing.T) {
				t.Parallel()

				db := dbConstructor.DbConstructor(t)
				schemaAdapter := newPostgresSchemaAdapter(0)
				topic := "topic_" + watermill.NewUUID()

				messagesToPublish := []*message.Message{
					message.NewMessage(watermill.NewUUID(), nil),
					message.NewMessage(watermill.NewUUID(), nil),
				}

				backoffManager := &recordingBackoffManager{}

				sub, err := sql.NewSubscriber(
					db,
					sql.SubscriberConfig{
						ConsumerGroup:  "test",
						BackoffManager: backoffManager,
						SchemaAdapter: failingDuringExecutionSchemaAdapter{
							DefaultPostgreSQLSchema: schemaAdapter,
							FailOnUUID:              messagesToPublish[tc.FailOnMessage].UUID,
						},
						OffsetsAdapter: newPostgresOffsetsAdapter(),
					},
					logger,
				)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, sub.Close())
				})

				require.NoError(t, sub.SubscribeInitialize(topic))

				publisher, err := sql.NewPublisher(db, sql.PublisherConfig{SchemaAdapter: schemaAdapter}, logger)
				require.NoError(t, err)
				require.NoError(t, publisher.Publish(topic, messagesToPublish...))

				received := subscribeAndAckAll(t, sub, topic)

				firstErr := backoffManager.waitForFirstError(t)
				assert.ErrorContains(t, firstErr, "division by zero")
				assert.Empty(t, received.UUIDs(), "messages from a failed query should not be delivered")
			})
		}
	}
}

// The select must fail while it executes, not when PostgreSQL parses or plans it: only then can the error
// come after rows were already read, and native pgx reports it only from rows.Err().
type failingDuringExecutionSchemaAdapter struct {
	*sql.DefaultPostgreSQLSchema
	FailOnUUID string
}

func (a failingDuringExecutionSchemaAdapter) SelectQuery(params sql.SelectQueryParams) (sql.Query, error) {
	query, err := a.DefaultPostgreSQLSchema.SelectQuery(params)
	if err != nil {
		return sql.Query{}, err
	}

	// The filter can't be moved below the LIMIT of the wrapped query,
	// so PostgreSQL evaluates it for each returned row, in order, while executing the query.
	query.Query = `SELECT * FROM (` + query.Query + `) AS messages
		WHERE 1 / (CASE WHEN uuid = '` + a.FailOnUUID + `' THEN 0 ELSE 1 END) = 1`

	return query, nil
}

func newFakeDBSubscriber(t *testing.T, db sql.Beginner, backoffManager sql.BackoffManager) *sql.Subscriber {
	t.Helper()

	sub, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			ConsumerGroup:  "test",
			BackoffManager: backoffManager,
			SchemaAdapter:  offsetOnlySchemaAdapter{},
			OffsetsAdapter: sql.DefaultPostgreSQLOffsetsAdapter{},
		},
		logger,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sub.Close())
	})

	return sub
}

func subscribeAndAckAll(t *testing.T, sub *sql.Subscriber, topic string) *receivedMessages {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	received := &receivedMessages{}
	go func() {
		for msg := range messages {
			received.add(msg.UUID)
			msg.Ack()
		}
	}()

	return received
}

type receivedMessages struct {
	lock  sync.Mutex
	uuids []string
}

func (r *receivedMessages) add(uuid string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.uuids = append(r.uuids, uuid)
}

func (r *receivedMessages) UUIDs() []string {
	r.lock.Lock()
	defer r.lock.Unlock()

	return append([]string(nil), r.uuids...)
}

type backoffCall struct {
	NoMsg bool
	Err   error
}

// The BackoffManager is the only place where the subscriber reports query errors, so the tests observe them here.
type recordingBackoffManager struct {
	lock  sync.Mutex
	calls []backoffCall
}

func (b *recordingBackoffManager) HandleError(_ watermill.LoggerAdapter, noMsg bool, err error) time.Duration {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.calls = append(b.calls, backoffCall{NoMsg: noMsg, Err: err})

	return 10 * time.Millisecond
}

func (b *recordingBackoffManager) Calls() []backoffCall {
	b.lock.Lock()
	defer b.lock.Unlock()

	return append([]backoffCall(nil), b.calls...)
}

func (b *recordingBackoffManager) waitForFirstCall(t *testing.T) backoffCall {
	t.Helper()

	require.Eventually(
		t,
		func() bool { return len(b.Calls()) > 0 },
		30*time.Second,
		10*time.Millisecond,
		"subscriber did not report any query result",
	)

	return b.Calls()[0]
}

func (b *recordingBackoffManager) waitForFirstError(t *testing.T) error {
	t.Helper()

	require.Eventually(
		t,
		func() bool { return b.firstError() != nil },
		30*time.Second,
		10*time.Millisecond,
		"subscriber did not report any error",
	)

	return b.firstError()
}

func (b *recordingBackoffManager) firstError() error {
	for _, call := range b.Calls() {
		if call.Err != nil {
			return call.Err
		}
	}

	return nil
}

type offsetOnlySchemaAdapter struct {
	sql.DefaultPostgreSQLSchema
}

func (offsetOnlySchemaAdapter) UnmarshalMessage(params sql.UnmarshalMessageParams) (sql.Row, error) {
	var offset int64
	if err := params.Row.Scan(&offset); err != nil {
		return sql.Row{}, err
	}

	return sql.Row{
		Offset: offset,
		Msg:    message.NewMessage(strconv.FormatInt(offset, 10), nil),
	}, nil
}

type fakeBeginner struct {
	newRows func() *fakeRows
}

func (b fakeBeginner) BeginTx(context.Context, *stdSQL.TxOptions) (sql.Tx, error) {
	return fakeTx{newRows: b.newRows}, nil
}

func (b fakeBeginner) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return fakeResult{}, nil
}

func (b fakeBeginner) QueryContext(context.Context, string, ...any) (sql.Rows, error) {
	return b.newRows(), nil
}

type fakeTx struct {
	newRows func() *fakeRows
}

func (t fakeTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return fakeResult{}, nil
}

func (t fakeTx) QueryContext(context.Context, string, ...any) (sql.Rows, error) {
	return t.newRows(), nil
}

func (t fakeTx) Commit() error {
	return nil
}

func (t fakeTx) Rollback() error {
	return nil
}

type fakeResult struct{}

func (fakeResult) RowsAffected() (int64, error) {
	return 0, nil
}

// Like database/sql.Rows and pgx.Rows, a query failing mid-way is reported only from Err, after Next returns false.
type fakeRows struct {
	offsets  []int64
	next     int
	err      error
	closeErr error
}

func (r *fakeRows) Next() bool {
	if r.next >= len(r.offsets) {
		return false
	}

	r.next++

	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	*dest[0].(*int64) = r.offsets[r.next-1]

	return nil
}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) Close() error {
	return r.closeErr
}

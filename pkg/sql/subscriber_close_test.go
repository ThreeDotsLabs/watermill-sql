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

// Generous, because the tests may run on a heavily loaded machine.
const closeTestTimeout = 30 * time.Second

func TestSubscriberClose_interrupts_blocked_select(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name string
		// Select waits like a SELECT waiting for the consumer group's lock, held by another subscriber.
		Select func(ctx context.Context, tx *closeTestTx) (sql.Rows, error)
	}{
		{
			// Like lib/pq and pgx's database/sql driver.
			Name: "blocked_in_QueryContext",
			Select: func(ctx context.Context, tx *closeTestTx) (sql.Rows, error) {
				<-ctx.Done()
				tx.breakConnection()
				return nil, ctx.Err()
			},
		},
		{
			// Like native pgx: an interrupted fetch looks like the end of the rows.
			Name: "blocked_in_rows_Next",
			Select: func(ctx context.Context, tx *closeTestTx) (sql.Rows, error) {
				return &closeTestRows{waitInNext: ctx, tx: tx}, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			db := &closeTestDB{selectFunc: tc.Select}
			logger := watermill.NewCaptureLogger()
			backoffManager := &closeTestBackoffManager{}
			sub := newCloseTestSubscriber(t, db, logger, backoffManager, time.Millisecond)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := sub.Subscribe(ctx, "topic")
			require.NoError(t, err)

			require.Eventually(t, func() bool { return db.selectsStarted() > 0 }, closeTestTimeout, time.Millisecond)

			// Canceling ctx unblocks the SELECT in case Close() does not.
			requireSubscriberCloseReturns(t, sub, cancel)

			assert.Empty(t, backoffManager.handledErrors(), "errors caused by closing should not be handled by BackoffManager")
			assert.Empty(t, logger.Captured()[watermill.ErrorLogLevel], "closing should not log errors")
		})
	}
}

func TestSubscriberClose_does_not_wait_for_ResendInterval(t *testing.T) {
	t.Parallel()

	db := &closeTestDB{selectFunc: closeTestSelectOnce(1)}
	logger := watermill.NewCaptureLogger()
	sub := newCloseTestSubscriber(t, db, logger, &closeTestBackoffManager{}, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := sub.Subscribe(ctx, "topic")
	require.NoError(t, err)

	closeTestReceive(t, messages).Nack()

	require.Eventually(t, func() bool {
		return closeTestLogged(logger, watermill.DebugLogLevel, "Message nacked, resending")
	}, closeTestTimeout, time.Millisecond, "the subscriber should start waiting before resending the message")

	requireSubscriberCloseReturns(t, sub, cancel)
}

// TestSubscriberClose_commits_acks_of_messages_acked_before_closing guards that closing the subscriber
// does not interrupt the acking of the messages acked before Close() was called.
func TestSubscriberClose_commits_acks_of_messages_acked_before_closing(t *testing.T) {
	t.Parallel()

	db := &closeTestDB{selectFunc: closeTestSelectOnce(1, 2, 3)}
	logger := watermill.NewCaptureLogger()
	sub := newCloseTestSubscriber(t, db, logger, &closeTestBackoffManager{}, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := sub.Subscribe(ctx, "topic")
	require.NoError(t, err)

	first := closeTestReceive(t, messages)
	assert.Equal(t, "1", first.UUID)
	first.Ack()

	second := closeTestReceive(t, messages)
	assert.Equal(t, "2", second.UUID)

	// The second message is still being processed.
	requireSubscriberCloseReturns(t, sub, cancel)

	ackQuery, err := sql.DefaultPostgreSQLOffsetsAdapter{}.AckMessageQuery(sql.AckMessageQueryParams{Topic: "topic"})
	require.NoError(t, err)

	committedAcks := db.committedStatements(ackQuery.Query)
	require.Len(t, committedAcks, 1, "the ack of the first message should be committed")
	assert.Equal(t, int64(1), committedAcks[0].Args[0])
	assert.Empty(t, logger.Captured()[watermill.ErrorLogLevel])
}

// TestSubscriberClose_closes_output_channel_before_returning checks that the output channel is already closed
// when Close() returns. The window in which it could still be open is tiny, so the test repeats
// subscribing and closing for a while.
func TestSubscriberClose_closes_output_channel_before_returning(t *testing.T) {
	t.Parallel()

	iterations := 0
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); iterations++ {
		db := &closeTestDB{selectFunc: closeTestSelectOnce()}
		sub := newCloseTestSubscriber(t, db, watermill.NopLogger{}, &closeTestBackoffManager{}, time.Millisecond)

		messages, err := sub.Subscribe(context.Background(), "topic")
		require.NoError(t, err)

		require.NoError(t, sub.Close())

		select {
		case _, ok := <-messages:
			require.False(t, ok, "no message expected")
		default:
			t.Fatalf("output channel not closed when Close() returned (after %d iterations)", iterations)
		}
	}

	t.Logf("%d iterations", iterations)
}

func TestSubscriberClose_during_Subscribe(t *testing.T) {
	t.Parallel()

	pausedQueries, err := sql.DefaultPostgreSQLOffsetsAdapter{}.BeforeSubscribingQueries(sql.BeforeSubscribingQueriesParams{
		Topic:         "paused",
		ConsumerGroup: "test",
	})
	require.NoError(t, err)
	require.Len(t, pausedQueries, 1)

	paused := make(chan struct{})
	resume := make(chan struct{})
	db := &closeTestDB{
		selectFunc: closeTestSelectOnce(),
		beforeExec: func(query string) {
			if query == pausedQueries[0].Query {
				close(paused)
				<-resume
			}
		},
	}
	sub := newCloseTestSubscriber(t, db, watermill.NopLogger{}, &closeTestBackoffManager{}, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A running subscription, so that Close() has something to wait for.
	running, err := sub.Subscribe(ctx, "running")
	require.NoError(t, err)

	type subscribeResult struct {
		messages <-chan *message.Message
		err      error
	}
	subscribed := make(chan subscribeResult, 1)
	go func() {
		messages, err := sub.Subscribe(ctx, "paused")
		subscribed <- subscribeResult{messages, err}
	}()

	select {
	case <-paused:
	case <-time.After(closeTestTimeout):
		t.Fatal("Subscribe did not execute its before subscribing query")
	}

	// Resuming Subscribe unblocks Close() in case it waits for that subscription.
	requireSubscriberCloseReturns(t, sub, func() { close(resume) })
	_, ok := <-running
	assert.False(t, ok, "the running subscription should be closed")

	close(resume)

	select {
	case result := <-subscribed:
		assert.ErrorIs(t, result.err, sql.ErrSubscriberClosed)
		assert.Nil(t, result.messages, "no subscription should be started after Close() returned")
	case <-time.After(closeTestTimeout):
		t.Fatal("Subscribe did not return")
	}
}

// TestSubscriberClose_concurrent_with_Subscribe is meant to be run with -race,
// which detects when Subscribe adds a subscription concurrently with Close() waiting for them.
func TestSubscriberClose_concurrent_with_Subscribe(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		db := &closeTestDB{selectFunc: closeTestSelectOnce()}
		sub := newCloseTestSubscriber(t, db, watermill.NopLogger{}, &closeTestBackoffManager{}, time.Millisecond)

		// A running subscription, so that Close() has something to wait for.
		_, err := sub.Subscribe(ctx, "running")
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			_, err := sub.Subscribe(ctx, "concurrent")
			if err != nil {
				assert.ErrorIs(t, err, sql.ErrSubscriberClosed)
			}
		}()
		go func() {
			defer wg.Done()

			assert.NoError(t, sub.Close())
		}()
		wg.Wait()
	}
}

func TestSubscriberClose_does_not_wait_for_consumer_group_lock(t *testing.T) {
	t.Parallel()

	for _, tc := range closeTestPostgresDrivers() {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			db := tc.DB(t)
			topic := "topic_" + watermill.NewUUID()
			offsetsAdapter := newPostgresOffsetsAdapter()

			newSubscriber := func(logger watermill.LoggerAdapter) *sql.Subscriber {
				// Longer than closeTestTimeout, so that the message is held until it's acked.
				ackDeadline := 2 * closeTestTimeout

				sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
					ConsumerGroup:  "group",
					AckDeadline:    &ackDeadline,
					PollInterval:   10 * time.Millisecond,
					ResendInterval: 10 * time.Millisecond,
					SchemaAdapter:  newPostgresSchemaAdapter(0),
					OffsetsAdapter: offsetsAdapter,
				}, logger)
				require.NoError(t, err)

				return sub
			}

			holdingSub := newSubscriber(logger)
			require.NoError(t, holdingSub.SubscribeInitialize(topic))

			publisher, err := sql.NewPublisher(db, sql.PublisherConfig{SchemaAdapter: newPostgresSchemaAdapter(0)}, logger)
			require.NoError(t, err)
			require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), nil)))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			holdingMessages, err := holdingSub.Subscribe(ctx, topic)
			require.NoError(t, err)

			// Until it's acked, the subscriber holds the lock on the consumer group's offsets row.
			heldMsg := closeTestReceive(t, holdingMessages)

			waitingLogger := watermill.NewCaptureLogger()
			waitingSub := newSubscriber(waitingLogger)
			_, err = waitingSub.Subscribe(ctx, topic)
			require.NoError(t, err)

			closeTestWaitForSelectWaitingForLock(t, offsetsAdapter.MessagesOffsetsTable(topic))

			// Acking the held message releases the lock in case Close() does not interrupt the waiting SELECT.
			requireSubscriberCloseReturns(t, waitingSub, func() { heldMsg.Ack() })
			assert.Empty(t, waitingLogger.Captured()[watermill.ErrorLogLevel], "closing should not log errors")

			heldMsg.Ack()
			requireSubscriberCloseReturns(t, holdingSub, cancel)
		})
	}
}

// TestSubscriberClose_commits_acks_of_messages_acked_before_closing_postgres guards that closing the subscriber
// does not interrupt the acking of the messages acked before Close() was called.
func TestSubscriberClose_commits_acks_of_messages_acked_before_closing_postgres(t *testing.T) {
	t.Parallel()

	for _, tc := range closeTestPostgresDrivers() {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			db := tc.DB(t)
			topic := "topic_" + watermill.NewUUID()

			newSubscriber := func() *sql.Subscriber {
				sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
					ConsumerGroup:  "group",
					PollInterval:   10 * time.Millisecond,
					ResendInterval: 10 * time.Millisecond,
					SchemaAdapter:  newPostgresSchemaAdapter(3),
					OffsetsAdapter: newPostgresOffsetsAdapter(),
				}, logger)
				require.NoError(t, err)

				return sub
			}

			sub := newSubscriber()
			require.NoError(t, sub.SubscribeInitialize(topic))

			publisher, err := sql.NewPublisher(db, sql.PublisherConfig{SchemaAdapter: newPostgresSchemaAdapter(3)}, logger)
			require.NoError(t, err)
			published := []*message.Message{
				message.NewMessage(watermill.NewUUID(), nil),
				message.NewMessage(watermill.NewUUID(), nil),
				message.NewMessage(watermill.NewUUID(), nil),
			}
			require.NoError(t, publisher.Publish(topic, published...))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			messages, err := sub.Subscribe(ctx, topic)
			require.NoError(t, err)

			first := closeTestReceive(t, messages)
			require.Equal(t, published[0].UUID, first.UUID)
			first.Ack()

			second := closeTestReceive(t, messages)
			require.Equal(t, published[1].UUID, second.UUID)

			// The second message is still being processed.
			requireSubscriberCloseReturns(t, sub, cancel)

			nextSub := newSubscriber()
			nextMessages, err := nextSub.Subscribe(ctx, topic)
			require.NoError(t, err)

			next := closeTestReceive(t, nextMessages)
			assert.Equal(t, published[1].UUID, next.UUID, "the ack of the first message should be committed")
			next.Ack()

			requireSubscriberCloseReturns(t, nextSub, cancel)
		})
	}
}

func closeTestPostgresDrivers() []struct {
	Name string
	DB   func(t *testing.T) sql.Beginner
} {
	return []struct {
		Name string
		DB   func(t *testing.T) sql.Beginner
	}{
		{Name: "lib_pq", DB: newPostgreSQL},
		{Name: "pgx_stdlib", DB: newPgxPostgreSQL},
		{Name: "pgx", DB: newPgx},
	}
}

func closeTestWaitForSelectWaitingForLock(t *testing.T, offsetsTable string) {
	t.Helper()

	db := newPostgreSQL(t)

	require.Eventually(t, func() bool {
		rows, err := db.QueryContext(
			context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0`,
			offsetsTable,
		)
		if err != nil {
			t.Logf("could not query pg_stat_activity: %s", err)
			return false
		}
		defer rows.Close()

		var waiting int
		if !rows.Next() || rows.Scan(&waiting) != nil {
			return false
		}

		return waiting > 0
	}, closeTestTimeout, 10*time.Millisecond, "the subscriber's SELECT should wait for the consumer group's lock")
}

// unblock lets a subscriber that Close() did not stop finish, so that it does not outlive the test.
func requireSubscriberCloseReturns(t *testing.T, sub *sql.Subscriber, unblock func()) {
	t.Helper()

	closed := make(chan error, 1)
	go func() {
		closed <- sub.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
		return
	case <-time.After(closeTestTimeout):
	}

	unblock()

	select {
	case <-closed:
		t.Fatalf("Close() did not return within %s, it returned only after unblocking", closeTestTimeout)
	case <-time.After(closeTestTimeout):
		t.Fatalf("Close() did not return within %s, not even after unblocking", closeTestTimeout)
	}
}

func closeTestReceive(t *testing.T, messages <-chan *message.Message) *message.Message {
	t.Helper()

	select {
	case msg, ok := <-messages:
		require.True(t, ok, "messages channel closed")
		return msg
	case <-time.After(closeTestTimeout):
		t.Fatal("no message received")
		return nil
	}
}

func closeTestLogged(logger *watermill.CaptureLoggerAdapter, level watermill.LogLevel, msg string) bool {
	for _, captured := range logger.Captured()[level] {
		if captured.Msg == msg {
			return true
		}
	}

	return false
}

func newCloseTestSubscriber(
	t *testing.T,
	db sql.Beginner,
	logger watermill.LoggerAdapter,
	backoffManager sql.BackoffManager,
	resendInterval time.Duration,
) *sql.Subscriber {
	t.Helper()

	sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		ConsumerGroup:  "test",
		ResendInterval: resendInterval,
		BackoffManager: backoffManager,
		SchemaAdapter:  sql.DefaultPostgreSQLSchema{},
		OffsetsAdapter: sql.DefaultPostgreSQLOffsetsAdapter{},
	}, logger)
	require.NoError(t, err)

	return sub
}

type closeTestBackoffManager struct {
	mu   sync.Mutex
	errs []error
}

func (b *closeTestBackoffManager) HandleError(_ watermill.LoggerAdapter, _ bool, err error) time.Duration {
	if err != nil {
		b.mu.Lock()
		b.errs = append(b.errs, err)
		b.mu.Unlock()
	}

	return time.Millisecond
}

func (b *closeTestBackoffManager) handledErrors() []error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]error(nil), b.errs...)
}

func closeTestSelectOnce(offsets ...int64) func(ctx context.Context, tx *closeTestTx) (sql.Rows, error) {
	var once sync.Once

	return func(ctx context.Context, tx *closeTestTx) (sql.Rows, error) {
		rows := &closeTestRows{}
		once.Do(func() {
			rows.offsets = offsets
		})

		return rows, nil
	}
}

// closeTestDB works only with DefaultPostgreSQLSchema and DefaultPostgreSQLOffsetsAdapter.
// Like the real drivers, it fails statements executed with a canceled context: the tests rely on it
// to catch acks executed with a context canceled by closing.
type closeTestDB struct {
	// The subscriber runs only its SELECT with QueryContext.
	selectFunc func(ctx context.Context, tx *closeTestTx) (sql.Rows, error)
	beforeExec func(query string)

	mu      sync.Mutex
	selects int
	txs     []*closeTestTx
}

func (db *closeTestDB) BeginTx(ctx context.Context, _ *stdSQL.TxOptions) (sql.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx := &closeTestTx{db: db, ctx: ctx}

	db.mu.Lock()
	db.txs = append(db.txs, tx)
	db.mu.Unlock()

	return tx, nil
}

func (db *closeTestDB) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("closeTestDB: statements outside of a transaction are not supported")
}

func (db *closeTestDB) QueryContext(context.Context, string, ...any) (sql.Rows, error) {
	return nil, errors.New("closeTestDB: queries outside of a transaction are not supported")
}

func (db *closeTestDB) selectsStarted() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.selects
}

func (db *closeTestDB) committedStatements(query string) []sql.Query {
	db.mu.Lock()
	defer db.mu.Unlock()

	var statements []sql.Query
	for _, tx := range db.txs {
		if !tx.committed {
			continue
		}
		for _, statement := range tx.statements {
			if statement.Query == query {
				statements = append(statements, statement)
			}
		}
	}

	return statements
}

type closeTestTx struct {
	db  *closeTestDB
	ctx context.Context

	// guarded by db.mu
	statements []sql.Query
	committed  bool
	done       bool
	broken     bool
}

// breakConnection makes the transaction fail like the real drivers, which close the connection to interrupt a query.
func (tx *closeTestTx) breakConnection() {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	tx.broken = true
}

func (tx *closeTestTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx.db.beforeExec != nil {
		tx.db.beforeExec(query)
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if err := tx.usableLocked(ctx); err != nil {
		return nil, err
	}

	tx.statements = append(tx.statements, sql.Query{Query: query, Args: args})

	return closeTestResult{}, nil
}

func (tx *closeTestTx) QueryContext(ctx context.Context, _ string, _ ...any) (sql.Rows, error) {
	tx.db.mu.Lock()
	err := tx.usableLocked(ctx)
	if err == nil {
		tx.db.selects++
	}
	tx.db.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return tx.db.selectFunc(ctx, tx)
}

func (tx *closeTestTx) Commit() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if err := tx.usableLocked(tx.ctx); err != nil {
		return err
	}

	tx.done = true
	tx.committed = true

	return nil
}

func (tx *closeTestTx) Rollback() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.done {
		return stdSQL.ErrTxDone
	}
	tx.done = true

	if tx.broken {
		return errors.New("closeTestTx: connection closed")
	}

	return nil
}

func (tx *closeTestTx) usableLocked(ctx context.Context) error {
	if tx.done {
		return stdSQL.ErrTxDone
	}
	if tx.broken {
		return errors.New("closeTestTx: connection closed")
	}

	return ctx.Err()
}

type closeTestResult struct{}

func (closeTestResult) RowsAffected() (int64, error) {
	return 1, nil
}

type closeTestRows struct {
	offsets []int64
	next    int

	waitInNext context.Context
	tx         *closeTestTx
}

func (r *closeTestRows) Next() bool {
	if r.waitInNext != nil {
		<-r.waitInNext.Done()
		r.tx.breakConnection()
		return false
	}

	if r.next >= len(r.offsets) {
		return false
	}
	r.next++

	return true
}

func (r *closeTestRows) Scan(dest ...any) error {
	if len(dest) != 5 {
		return errors.New("closeTestRows: unexpected number of columns")
	}

	offset := r.offsets[r.next-1]
	uuid := []byte(strconv.FormatInt(offset, 10))

	*dest[0].(*int64) = offset
	*dest[1].(*sql.XID8) = 1
	*dest[2].(*[]byte) = uuid
	*dest[3].(*[]byte) = nil
	*dest[4].(*[]byte) = nil

	return nil
}

func (r *closeTestRows) Close() error {
	return nil
}

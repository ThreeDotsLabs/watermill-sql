package sql

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type mockBeginner struct {
	tx *mockTx
}

func (b *mockBeginner) BeginTx(context.Context, *stdSQL.TxOptions) (Tx, error) {
	return b.tx, nil
}

func (b *mockBeginner) ExecContext(context.Context, string, ...any) (Result, error) {
	panic("unexpected ExecContext call on beginner")
}

func (b *mockBeginner) QueryContext(context.Context, string, ...any) (Rows, error) {
	panic("unexpected QueryContext call on beginner")
}

type executedQuery struct {
	query string
	args  []any
}

type mockTx struct {
	mu              sync.Mutex
	rows            Rows
	committed       bool
	rolledBack      bool
	executedQueries []executedQuery
}

func (tx *mockTx) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.executedQueries = append(tx.executedQueries, executedQuery{query: query, args: args})
	return mockResult{}, nil
}

func (tx *mockTx) QueryContext(context.Context, string, ...any) (Rows, error) {
	return tx.rows, nil
}

func (tx *mockTx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.rolledBack = true
	return nil
}

func (tx *mockTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.committed = true
	return nil
}

type mockResult struct{}

func (mockResult) RowsAffected() (int64, error) {
	return 1, nil
}

type mockRows struct {
	index int
	rows  []Row
}

func (r *mockRows) Scan(...any) error {
	return nil
}

func (r *mockRows) Close() error {
	return nil
}

func (r *mockRows) Next() bool {
	if r.index < len(r.rows) {
		r.index++
		return true
	}
	return false
}

type mockSchemaAdapter struct {
	rows []Row
}

func (mockSchemaAdapter) InsertQuery(InsertQueryParams) (Query, error) {
	panic("unexpected InsertQuery call")
}

func (mockSchemaAdapter) SelectQuery(SelectQueryParams) (Query, error) {
	return Query{Query: "SELECT messages"}, nil
}

func (a *mockSchemaAdapter) UnmarshalMessage(params UnmarshalMessageParams) (Row, error) {
	mr, ok := params.Row.(*mockRows)
	if !ok {
		return Row{}, fmt.Errorf("expected *mockRows, got %T", params.Row)
	}
	if mr.index-1 < len(a.rows) {
		return a.rows[mr.index-1], nil
	}
	return Row{}, stdSQL.ErrNoRows
}

func (mockSchemaAdapter) SchemaInitializingQueries(SchemaInitializingQueriesParams) ([]Query, error) {
	panic("unexpected SchemaInitializingQueries call")
}

func (mockSchemaAdapter) SubscribeIsolationLevel() stdSQL.IsolationLevel {
	return stdSQL.LevelReadCommitted
}

type mockOffsetsAdapter struct {
	mu           sync.Mutex
	ackCalls     []AckMessageQueryParams
	ackQueryFunc func(params AckMessageQueryParams) (Query, error)
}

func (a *mockOffsetsAdapter) AckMessageQuery(params AckMessageQueryParams) (Query, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ackCalls = append(a.ackCalls, params)
	if a.ackQueryFunc != nil {
		return a.ackQueryFunc(params)
	}
	return Query{Query: "DELETE FROM messages", Args: []any{"test"}}, nil
}

func (*mockOffsetsAdapter) ConsumedMessageQuery(ConsumedMessageQueryParams) (Query, error) {
	return Query{}, nil
}

func (*mockOffsetsAdapter) NextOffsetQuery(NextOffsetQueryParams) (Query, error) {
	panic("unexpected NextOffsetQuery call")
}

func (*mockOffsetsAdapter) SchemaInitializingQueries(OffsetsSchemaInitializingQueriesParams) ([]Query, error) {
	panic("unexpected SchemaInitializingQueries call")
}

func (*mockOffsetsAdapter) BeforeSubscribingQueries(BeforeSubscribingQueriesParams) ([]Query, error) {
	panic("unexpected BeforeSubscribingQueries call")
}

func TestSubscriberQuery_AckDeadlinePartialBatchPreservesUnacked(t *testing.T) {
	numMessages := 5
	rows := make([]Row, numMessages)
	for i := 0; i < numMessages; i++ {
		rows[i] = Row{
			Offset: int64(i + 1),
			Msg:    message.NewMessage(fmt.Sprintf("msg-%d", i+1), []byte(fmt.Sprintf("payload-%d", i+1))),
		}
	}

	mockR := &mockRows{rows: rows}
	tx := &mockTx{rows: mockR}
	db := &mockBeginner{tx: tx}
	offsets := &mockOffsetsAdapter{}
	schema := &mockSchemaAdapter{rows: rows}

	ackDeadline := 100 * time.Millisecond
	sub := &Subscriber{
		consumerIdBytes: []byte("consumer-id"),
		db:              db,
		config: SubscriberConfig{
			AckDeadline:    &ackDeadline,
			ResendInterval: 10 * time.Millisecond,
			SchemaAdapter:  schema,
			OffsetsAdapter: offsets,
		},
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan *message.Message)

	type queryResult struct {
		noMsg bool
		err   error
	}
	resultCh := make(chan queryResult, 1)

	go func() {
		noMsg, err := sub.query(ctx, "topic", out, watermill.NopLogger{})
		resultCh <- queryResult{noMsg: noMsg, err: err}
	}()

	// Read msg-1 and Ack immediately
	select {
	case msg1 := <-out:
		require.Equal(t, "msg-1", msg1.UUID)
		msg1.Ack()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg-1")
	}

	// Read msg-2 and Ack immediately
	select {
	case msg2 := <-out:
		require.Equal(t, "msg-2", msg2.UUID)
		msg2.Ack()
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg-2")
	}

	// Read msg-3 and do NOT Ack before AckDeadline (100ms) expires
	select {
	case msg3 := <-out:
		require.Equal(t, "msg-3", msg3.UUID)
		// Wait longer than ackDeadline without calling Ack()
		time.Sleep(150 * time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg-3")
	}

	// The query should complete now
	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		assert.False(t, res.noMsg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sub.query to return")
	}

	// Ensure msg-4 and msg-5 were never sent
	select {
	case unexpected := <-out:
		t.Fatalf("unexpected message received: %s", unexpected.UUID)
	default:
	}

	// Verify only msg-1 and msg-2 were provided to AckMessageQuery
	offsets.mu.Lock()
	require.Len(t, offsets.ackCalls, 1)
	callParams := offsets.ackCalls[0]
	offsets.mu.Unlock()

	require.Len(t, callParams.Rows, 2, "only acked rows must be passed to AckMessageQuery")
	assert.Equal(t, int64(1), callParams.Rows[0].Offset)
	assert.Equal(t, int64(2), callParams.Rows[1].Offset)
	assert.Equal(t, int64(2), callParams.LastRow.Offset)

	tx.mu.Lock()
	assert.True(t, tx.committed)
	assert.False(t, tx.rolledBack)
	require.Len(t, tx.executedQueries, 1)
	assert.Equal(t, "DELETE FROM messages", tx.executedQueries[0].query)
	tx.mu.Unlock()
}

func TestSubscriberQuery_FirstMessageTimeoutDoesNotAck(t *testing.T) {
	numMessages := 3
	rows := make([]Row, numMessages)
	for i := 0; i < numMessages; i++ {
		rows[i] = Row{
			Offset: int64(i + 1),
			Msg:    message.NewMessage(fmt.Sprintf("msg-%d", i+1), []byte(fmt.Sprintf("payload-%d", i+1))),
		}
	}

	mockR := &mockRows{rows: rows}
	tx := &mockTx{rows: mockR}
	db := &mockBeginner{tx: tx}
	offsets := &mockOffsetsAdapter{}
	schema := &mockSchemaAdapter{rows: rows}

	ackDeadline := 100 * time.Millisecond
	sub := &Subscriber{
		consumerIdBytes: []byte("consumer-id"),
		db:              db,
		config: SubscriberConfig{
			AckDeadline:    &ackDeadline,
			ResendInterval: 10 * time.Millisecond,
			SchemaAdapter:  schema,
			OffsetsAdapter: offsets,
		},
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan *message.Message)

	type queryResult struct {
		noMsg bool
		err   error
	}
	resultCh := make(chan queryResult, 1)

	go func() {
		noMsg, err := sub.query(ctx, "topic", out, watermill.NopLogger{})
		resultCh <- queryResult{noMsg: noMsg, err: err}
	}()

	// Read msg-1 and let ackDeadline expire
	select {
	case msg1 := <-out:
		require.Equal(t, "msg-1", msg1.UUID)
		time.Sleep(150 * time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg-1")
	}

	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		assert.True(t, res.noMsg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sub.query to return")
	}

	// OffsetsAdapter.AckMessageQuery must not be called when 0 messages acked
	offsets.mu.Lock()
	assert.Empty(t, offsets.ackCalls)
	offsets.mu.Unlock()

	tx.mu.Lock()
	assert.True(t, tx.committed)
	assert.False(t, tx.rolledBack)
	assert.Empty(t, tx.executedQueries)
	tx.mu.Unlock()
}

func TestSubscriberQuery_AllMessagesAcked(t *testing.T) {
	numMessages := 3
	rows := make([]Row, numMessages)
	for i := 0; i < numMessages; i++ {
		rows[i] = Row{
			Offset: int64(i + 1),
			Msg:    message.NewMessage(fmt.Sprintf("msg-%d", i+1), []byte(fmt.Sprintf("payload-%d", i+1))),
		}
	}

	mockR := &mockRows{rows: rows}
	tx := &mockTx{rows: mockR}
	db := &mockBeginner{tx: tx}
	offsets := &mockOffsetsAdapter{}
	schema := &mockSchemaAdapter{rows: rows}

	ackDeadline := 100 * time.Millisecond
	sub := &Subscriber{
		consumerIdBytes: []byte("consumer-id"),
		db:              db,
		config: SubscriberConfig{
			AckDeadline:    &ackDeadline,
			ResendInterval: 10 * time.Millisecond,
			SchemaAdapter:  schema,
			OffsetsAdapter: offsets,
		},
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan *message.Message)

	type queryResult struct {
		noMsg bool
		err   error
	}
	resultCh := make(chan queryResult, 1)

	go func() {
		noMsg, err := sub.query(ctx, "topic", out, watermill.NopLogger{})
		resultCh <- queryResult{noMsg: noMsg, err: err}
	}()

	for i := 0; i < numMessages; i++ {
		select {
		case msg := <-out:
			msg.Ack()
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		assert.False(t, res.noMsg)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sub.query to return")
	}

	offsets.mu.Lock()
	require.Len(t, offsets.ackCalls, 1)
	callParams := offsets.ackCalls[0]
	offsets.mu.Unlock()

	require.Len(t, callParams.Rows, 3)
	assert.Equal(t, int64(3), callParams.LastRow.Offset)

	tx.mu.Lock()
	assert.True(t, tx.committed)
	assert.False(t, tx.rolledBack)
	require.Len(t, tx.executedQueries, 1)
	tx.mu.Unlock()
}

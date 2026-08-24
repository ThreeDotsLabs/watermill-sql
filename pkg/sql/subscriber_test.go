package sql

import (
	"context"
	stdSQL "database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestSubscriberQueryRollsBackOnNack(t *testing.T) {
	msg := message.NewMessage("message-id", []byte("payload"))
	rows := &nackTestRows{}
	tx := &nackTestTx{rows: rows}
	db := &nackTestBeginner{tx: tx}
	ackDeadline := time.Second

	subscriber := &Subscriber{
		consumerIdBytes: []byte("consumer-id"),
		db:              db,
		config: SubscriberConfig{
			AckDeadline:    &ackDeadline,
			ResendInterval: time.Millisecond,
			SchemaAdapter:  nackTestSchemaAdapter{msg: msg},
			OffsetsAdapter: nackTestOffsetsAdapter{},
		},
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	type queryResult struct {
		noMsg bool
		err   error
	}
	resultCh := make(chan queryResult, 1)
	out := make(chan *message.Message)

	go func() {
		noMsg, err := subscriber.query(ctx, "topic", out, watermill.NopLogger{})
		resultCh <- queryResult{noMsg: noMsg, err: err}
	}()

	select {
	case received := <-out:
		require.Same(t, msg, received)
		received.Nack()
	case <-time.After(time.Second):
		t.Fatal("message was not delivered")
	}

	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, errMessageNacked)
		require.False(t, result.noMsg)
		require.True(t, tx.rolledBack)
		require.False(t, tx.committed)
	case <-time.After(time.Second):
		t.Fatal("query did not return after the message was nacked")
	}
}

type nackTestBeginner struct {
	tx *nackTestTx
}

func (b *nackTestBeginner) BeginTx(context.Context, *stdSQL.TxOptions) (Tx, error) {
	return b.tx, nil
}

func (b *nackTestBeginner) ExecContext(context.Context, string, ...any) (Result, error) {
	panic("unexpected ExecContext call")
}

func (b *nackTestBeginner) QueryContext(context.Context, string, ...any) (Rows, error) {
	panic("unexpected QueryContext call")
}

type nackTestTx struct {
	rows       Rows
	committed  bool
	rolledBack bool
}

func (tx *nackTestTx) ExecContext(context.Context, string, ...any) (Result, error) {
	panic("unexpected ExecContext call")
}

func (tx *nackTestTx) QueryContext(context.Context, string, ...any) (Rows, error) {
	return tx.rows, nil
}

func (tx *nackTestTx) Rollback() error {
	tx.rolledBack = true
	return nil
}

func (tx *nackTestTx) Commit() error {
	tx.committed = true
	return nil
}

type nackTestRows struct {
	read bool
}

func (r *nackTestRows) Scan(...any) error {
	panic("unexpected Scan call")
}

func (r *nackTestRows) Close() error {
	return nil
}

func (r *nackTestRows) Next() bool {
	if r.read {
		return false
	}
	r.read = true
	return true
}

type nackTestSchemaAdapter struct {
	msg *message.Message
}

func (nackTestSchemaAdapter) InsertQuery(InsertQueryParams) (Query, error) {
	panic("unexpected InsertQuery call")
}

func (nackTestSchemaAdapter) SelectQuery(SelectQueryParams) (Query, error) {
	return Query{Query: "SELECT message"}, nil
}

func (a nackTestSchemaAdapter) UnmarshalMessage(UnmarshalMessageParams) (Row, error) {
	return Row{Offset: 1, Msg: a.msg}, nil
}

func (nackTestSchemaAdapter) SchemaInitializingQueries(SchemaInitializingQueriesParams) ([]Query, error) {
	panic("unexpected SchemaInitializingQueries call")
}

func (nackTestSchemaAdapter) SubscribeIsolationLevel() stdSQL.IsolationLevel {
	return stdSQL.LevelReadCommitted
}

type nackTestOffsetsAdapter struct{}

func (nackTestOffsetsAdapter) AckMessageQuery(AckMessageQueryParams) (Query, error) {
	panic("unexpected AckMessageQuery call")
}

func (nackTestOffsetsAdapter) ConsumedMessageQuery(ConsumedMessageQueryParams) (Query, error) {
	return Query{}, nil
}

func (nackTestOffsetsAdapter) NextOffsetQuery(NextOffsetQueryParams) (Query, error) {
	panic("unexpected NextOffsetQuery call")
}

func (nackTestOffsetsAdapter) SchemaInitializingQueries(OffsetsSchemaInitializingQueriesParams) ([]Query, error) {
	panic("unexpected SchemaInitializingQueries call")
}

func (nackTestOffsetsAdapter) BeforeSubscribingQueries(BeforeSubscribingQueriesParams) ([]Query, error) {
	panic("unexpected BeforeSubscribingQueries call")
}

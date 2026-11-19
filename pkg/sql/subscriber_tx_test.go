package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type txQueryTest struct {
	sub     *Subscriber
	tx      *mockTx
	offsets *mockOffsetsAdapter
	out     chan *message.Message
	result  chan error
}

func startTxQueryTest(t *testing.T, numMessages int) txQueryTest {
	t.Helper()

	rows := make([]Row, numMessages)
	for i := range numMessages {
		rows[i] = Row{
			Offset: int64(i + 1),
			Msg:    message.NewMessage(fmt.Sprintf("msg-%d", i+1), nil),
		}
	}

	tx := &mockTx{rows: &mockRows{rows: rows}}
	offsets := &mockOffsetsAdapter{}

	ackDeadline := 100 * time.Millisecond
	sub := &Subscriber{
		consumerIdBytes: []byte("consumer-id"),
		db:              &mockBeginner{tx: tx},
		config: SubscriberConfig{
			AckDeadline:    &ackDeadline,
			ResendInterval: 10 * time.Millisecond,
			SchemaAdapter:  &mockSchemaAdapter{rows: rows},
			OffsetsAdapter: offsets,
		},
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	test := txQueryTest{
		sub:     sub,
		tx:      tx,
		offsets: offsets,
		out:     make(chan *message.Message),
		result:  make(chan error, 1),
	}

	go func() {
		_, err := sub.query(ctx, "topic", test.out, watermill.NopLogger{})
		test.result <- err
	}()

	return test
}

func (q txQueryTest) receive(t *testing.T, uuid string) *message.Message {
	t.Helper()

	select {
	case msg := <-q.out:
		require.Equal(t, uuid, msg.UUID)
		return msg
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s", uuid)
		return nil
	}
}

func (q txQueryTest) requireFinished(t *testing.T) {
	t.Helper()

	select {
	case err := <-q.result:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sub.query to return")
	}

	select {
	case unexpected := <-q.out:
		t.Fatalf("unexpected message received: %s", unexpected.UUID)
	default:
	}
}

func (q txQueryTest) requireRolledBack(t *testing.T) {
	t.Helper()

	q.offsets.mu.Lock()
	assert.Empty(t, q.offsets.ackCalls, "nothing should be acked")
	q.offsets.mu.Unlock()

	q.tx.mu.Lock()
	defer q.tx.mu.Unlock()
	assert.True(t, q.tx.rolledBack)
	assert.False(t, q.tx.committed)
	assert.Empty(t, q.tx.executedQueries)
}

func TestSubscriberQuery_TxUsed_RollsBackBatchWhenNotAcked(t *testing.T) {
	testCases := []struct {
		Name     string
		NotAcked func(q txQueryTest, msg *message.Message)
	}{
		{
			Name: "nack",
			NotAcked: func(q txQueryTest, msg *message.Message) {
				msg.Nack()
			},
		},
		{
			Name: "ack_deadline",
			NotAcked: func(q txQueryTest, msg *message.Message) {
				<-msg.Context().Done()
			},
		},
		{
			Name: "close",
			NotAcked: func(q txQueryTest, msg *message.Message) {
				close(q.sub.closing)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			q := startTxQueryTest(t, 3)

			q.receive(t, "msg-1").Ack()

			msg2 := q.receive(t, "msg-2")
			_, ok := TxFromContext(msg2.Context())
			require.True(t, ok)
			tc.NotAcked(q, msg2)

			q.requireFinished(t)
			q.requireRolledBack(t)
		})
	}
}

// The message that used the transaction was acked; the nacked one didn't use it, so nothing has to be rolled back.
func TestSubscriberQuery_TxUsedByOtherMessage_NackResendsInSameTx(t *testing.T) {
	q := startTxQueryTest(t, 3)

	msg1 := q.receive(t, "msg-1")
	_, ok := TxFromContext(msg1.Context())
	require.True(t, ok)
	msg1.Ack()

	q.receive(t, "msg-2").Nack()
	q.receive(t, "msg-2").Ack()
	q.receive(t, "msg-3").Ack()

	q.requireFinished(t)

	q.offsets.mu.Lock()
	require.Len(t, q.offsets.ackCalls, 1)
	assert.Len(t, q.offsets.ackCalls[0].Rows, 3)
	q.offsets.mu.Unlock()

	q.tx.mu.Lock()
	defer q.tx.mu.Unlock()
	assert.True(t, q.tx.committed)
	assert.False(t, q.tx.rolledBack)
}

func TestTxFromContext_WithoutMessageContext(t *testing.T) {
	tx, ok := TxFromContext(context.Background())
	assert.False(t, ok)
	assert.Nil(t, tx)
}

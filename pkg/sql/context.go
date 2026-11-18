package sql

import (
	"context"
	"sync/atomic"
)

type contextKey string

const (
	txContextKey contextKey = "tx"
)

type messageTx struct {
	tx Tx

	// Without it, every nack would roll back the batch, and messages acked earlier in the batch would be
	// delivered again also to consumers that never write through the transaction.
	used atomic.Bool
}

func setTxToContext(ctx context.Context, mtx *messageTx) context.Context {
	return context.WithValue(ctx, txContextKey, mtx)
}

// TxFromContext returns the transaction used by the subscriber to consume the message.
// Changes made through it are committed together with the message's offset, only when the message is acked.
//
// The subscriber reads messages in batches (SubscribeBatchSize of the schema adapter, 100 by default)
// and consumes a whole batch in one transaction. When the handler of a message called TxFromContext,
// and that message is nacked, is not acked within AckDeadline, or the subscriber is closed before the ack,
// the whole transaction is rolled back and the batch is read again. Messages acked earlier in the same batch
// are then delivered again, and their changes made through the transaction are rolled back too.
// Set SubscribeBatchSize to 1 if that is a problem, for example when handlers have side effects outside the database.
//
// Call TxFromContext for each message, and don't keep the transaction for later messages:
// a nack of a message whose handler didn't call it doesn't roll anything back.
//
// It is useful when you want to ensure that data is updated only when the message is processed.
// Example usage: https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/exactly-once-delivery-counter
func TxFromContext(ctx context.Context) (Tx, bool) {
	mtx, ok := ctx.Value(txContextKey).(*messageTx)
	if !ok {
		return nil, false
	}

	mtx.used.Store(true)

	return mtx.tx, true
}

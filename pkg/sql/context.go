package sql

import (
	"context"
	"database/sql"
)

type contextKey string

const (
	txContextKey contextKey = "tx"
)

func setTxToContext(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txContextKey, tx)
}

// TxFromContext returns the transaction used by the subscriber to consume the message.
// The transaction will be committed if ack of the message is successful.
// When a nack is sent, the transaction will be rolled back.
//
// It is useful when you want to ensure that data is updated only when the message is processed.
// Example usage: https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/exactly-once-delivery-counter
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey).(*sql.Tx)
	return tx, ok
}

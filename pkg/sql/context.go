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

// TxFromContext returns transaction used by subscriber to consume message.
// The transaction will be committed if ack of message will be successful.
// When a nack is sent, the transaction will be rolled back.
//
// It is useful, when you want to ensure that data is update only when message will be processed.
// Example usage: https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/exactly-once-delivery-counter
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey).(*sql.Tx)
	return tx, ok
}

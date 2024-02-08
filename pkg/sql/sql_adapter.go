package sql

import (
	"context"
	"database/sql"
)

type StdSQLBeginner struct {
	*sql.DB
}

type StdSQLTx struct {
	*sql.Tx
}

// BeginTx converts the stdSQL.Tx struct to our Tx interface
func (c *StdSQLBeginner) BeginTx(ctx context.Context, options *sql.TxOptions) (Tx, error) {
	tx, err := c.DB.BeginTx(ctx, options)

	return &StdSQLTx{tx}, err
}

// ExecContext converts the stdSQL.Result struct to our Result interface
func (c *StdSQLBeginner) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return c.DB.ExecContext(ctx, query, args...)
}

// QueryContext converts the stdSQL.Rows struct to our Rows interface
func (c *StdSQLBeginner) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

// ExecContext converts the stdSQL.Result struct to our Result interface
func (t *StdSQLTx) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	return t.Tx.ExecContext(ctx, query, args...)
}

// QueryContext converts the stdSQL.Rows struct to our Rows interface
func (t *StdSQLTx) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	return t.Tx.QueryContext(ctx, query, args...)
}

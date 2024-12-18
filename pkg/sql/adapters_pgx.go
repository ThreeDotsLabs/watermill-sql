package sql

import (
	"context"
	stdSQL "database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Conn interface {
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)
}

type PgxBeginner struct {
	Conn
}

func BeginnerFromPgx(conn Conn) Beginner {
	return PgxBeginner{conn}
}

type PgxTx struct {
	pgx.Tx
	ctx context.Context
}

type PgxResult struct {
	pgconn.CommandTag
}

type PgxRows struct {
	pgx.Rows
}

func (c PgxBeginner) BeginTx(ctx context.Context, options *stdSQL.TxOptions) (Tx, error) {
	opts := pgx.TxOptions{}
	if options != nil {
		iso, err := toPgxIsolationLevel(options.Isolation)
		if err != nil {
			return nil, err
		}

		opts = pgx.TxOptions{
			IsoLevel:       iso,
			AccessMode:     toPgxAccessMode(options.ReadOnly),
			DeferrableMode: "",
		}
	}

	tx, err := c.Conn.BeginTx(ctx, opts)

	return &PgxTx{
		Tx:  tx,
		ctx: ctx,
	}, err
}

func (c PgxBeginner) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	res, err := c.Conn.Exec(ctx, query, args...)

	return PgxResult{res}, err
}

func (c PgxBeginner) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.Conn.Query(ctx, query, args...)

	return PgxRows{rows}, err
}

func (t PgxTx) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	res, err := t.Tx.Exec(ctx, query, args...)

	return PgxResult{res}, err
}

func (t PgxTx) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)

	return PgxRows{rows}, err
}

func (t PgxTx) Rollback() error {
	return t.Tx.Rollback(context.WithoutCancel(t.ctx))
}

func (t PgxTx) Commit() error {
	return t.Tx.Commit(t.ctx)
}

func (p PgxResult) RowsAffected() (int64, error) {
	return p.CommandTag.RowsAffected(), nil
}

func (p PgxRows) Close() error {
	p.Rows.Close()

	return nil
}

func toPgxIsolationLevel(level stdSQL.IsolationLevel) (pgx.TxIsoLevel, error) {
	switch level {
	case stdSQL.LevelReadUncommitted:
		return pgx.ReadUncommitted, nil
	case stdSQL.LevelReadCommitted:
		return pgx.ReadCommitted, nil
	case stdSQL.LevelRepeatableRead:
		return pgx.RepeatableRead, nil
	case stdSQL.LevelSerializable:
		return pgx.Serializable, nil
	case stdSQL.LevelSnapshot:
		return pgx.Serializable, fmt.Errorf("pgx does not support snapshot isolation")
	default:
		return pgx.Serializable, nil
	}
}

func toPgxAccessMode(readOnly bool) pgx.TxAccessMode {
	if readOnly {
		return pgx.ReadOnly
	}

	return pgx.ReadWrite
}

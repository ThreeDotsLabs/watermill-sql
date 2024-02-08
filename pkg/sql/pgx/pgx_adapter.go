package pgx

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"time"

	"github.com/julesjcraske/watermill-sql/v3/pkg/sql"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Conn interface {
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
}

type Beginner struct {
	Conn
}

type Tx struct {
	pgx.Tx
}

type Result struct {
	pgconn.CommandTag
}

type Rows struct {
	pgx.Rows
}

func (c Beginner) BeginTx(ctx context.Context, options *stdSQL.TxOptions) (sql.Tx, error) {
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

	return Tx{tx}, err
}

func (c Beginner) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	res, err := c.Conn.Exec(ctx, query, args...)

	return Result{res}, err
}

func (c Beginner) QueryContext(ctx context.Context, query string, args ...interface{}) (sql.Rows, error) {
	rows, err := c.Conn.Query(ctx, query, args...)

	return Rows{rows}, err
}

func (t Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := t.Tx.Exec(ctx, query, args...)

	return Result{res}, err
}

func (t Tx) QueryContext(ctx context.Context, query string, args ...any) (sql.Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)

	return Rows{rows}, err
}

func (t Tx) Rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return t.Tx.Rollback(ctx)
}

func (t Tx) Commit() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return t.Tx.Commit(ctx)
}

func (p Result) RowsAffected() (int64, error) {
	return p.CommandTag.RowsAffected(), nil
}

func (p Rows) Close() error {
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

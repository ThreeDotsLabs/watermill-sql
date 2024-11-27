package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// A Result summarizes an executed SQL command.
type Result interface {
	// RowsAffected returns the number of rows affected by an
	// update, insert, or delete. Not every database or database
	// driver may support this.
	RowsAffected() (int64, error)
}

type Rows interface {
	Scan(dest ...any) error
	Close() error
	Next() bool
}

type Tx interface {
	ContextExecutor
	Rollback() error
	Commit() error
}

// ContextExecutor can perform SQL queries with context
type ContextExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

// Beginner begins transactions.
type Beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (Tx, error)
	ContextExecutor
}

// SQLBeginner matches the standard library sql.DB and sql.Tx interfaces.
type SQLBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// sqlArgsToLog is used for "lazy" generating sql args strings to logger
type sqlArgsToLog []interface{}

func (s sqlArgsToLog) String() string {
	var strArgs []string
	for _, arg := range s {
		strArgs = append(strArgs, fmt.Sprintf("%v", arg))
	}

	return strings.Join(strArgs, ",")
}

type Scanner interface {
	Scan(dest ...any) error
}

type Query struct {
	Query string
	Args  []any
}

func (q Query) IsZero() bool {
	return q.Query == ""
}

func (q Query) String() string {
	return fmt.Sprintf("%s %s", q.Query, sqlArgsToLog(q.Args))
}

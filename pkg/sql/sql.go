package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// interface definitions borrowed from github.com/volatiletech/sqlboiler

// Executor can perform SQL queries.
type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// ContextExecutor can perform SQL queries with context
type ContextExecutor interface {
	Executor

	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Beginner begins transactions.
type Beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ContextExecutor
}

// sqlArgsToLog is used for "lazy" generating sql args strings to logger
type sqlArgsToLog []interface{}

func (s sqlArgsToLog) String() string {
	var strArgs []string
	for _, arg := range s {
		strArgs = append(strArgs, fmt.Sprintf("%s", arg))
	}

	return strings.Join(strArgs, ",")
}

type Scanner interface {
	Scan(dest ...any) error
}

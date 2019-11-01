package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// interface definitions borrowed from github.com/volatiletech/sqlboiler

// executor can perform SQL queries.
type executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// contextExecutor can perform SQL queries with context
type contextExecutor interface {
	executor

	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// beginner begins transactions.
type beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	contextExecutor
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

func isDeadlock(err error) bool {
	// ugly, but should be universal for multiple sql implementations
	return strings.Contains(strings.ToLower(err.Error()), "deadlock")
}

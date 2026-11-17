package sql_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestXID8_as_query_argument checks that XID8 can be written to and compared with an xid8 column,
// both by value (as DefaultPostgreSQLOffsetsAdapter.AckMessageQuery does) and as a pointer
// (as custom offsets adapters may do).
func TestXID8_as_query_argument(t *testing.T) {
	t.Parallel()

	databases := []struct {
		Name string
		DB   sql.Beginner
	}{
		{
			Name: "lib/pq",
			DB:   newPostgreSQL(t),
		},
		{
			Name: "pgx stdlib",
			DB:   newPgxPostgreSQL(t),
		},
		{
			Name: "pgx",
			DB:   sql.BeginnerFromPgx(newPgxPool(t)),
		},
	}

	xids := []sql.XID8{
		732406,
		math.MaxInt64,
		math.MaxInt64 + 1,
		math.MaxUint64,
	}

	for _, db := range databases {
		for _, xid := range xids {
			args := []struct {
				Name string
				Arg  any
			}{
				{
					Name: "pointer",
					Arg:  &xid,
				},
				{
					Name: "value",
					Arg:  xid,
				},
			}

			for _, arg := range args {
				t.Run(fmt.Sprintf("%s/%s/%d", db.Name, arg.Name, uint64(xid)), func(t *testing.T) {
					t.Parallel()

					ctx := context.Background()

					tx, err := db.DB.BeginTx(ctx, nil)
					require.NoError(t, err)
					defer func() { _ = tx.Rollback() }()

					// A temporary table is visible only to this connection and is dropped with the transaction.
					_, err = tx.ExecContext(ctx, `CREATE TEMPORARY TABLE xid8_query_args_test (transaction_id xid8 NOT NULL) ON COMMIT DROP`)
					require.NoError(t, err)

					_, err = tx.ExecContext(ctx, `INSERT INTO xid8_query_args_test (transaction_id) VALUES ($1)`, arg.Arg)
					require.NoError(t, err)

					// The text form is read back so that the test checks only how the argument is sent, not XID8.Scan.
					rows, err := tx.QueryContext(
						ctx,
						`SELECT transaction_id::text FROM xid8_query_args_test WHERE transaction_id = $1`,
						arg.Arg,
					)
					require.NoError(t, err)
					defer func() { _ = rows.Close() }()

					require.True(t, rows.Next(), "no row matched the XID8 argument")

					var stored string
					require.NoError(t, rows.Scan(&stored))
					assert.Equal(t, strconv.FormatUint(uint64(xid), 10), stored)
				})
			}
		}
	}
}

package sql_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The literal is covered too: PgxTx is exported, so users can build it without TxFromPgx.
var pgxTxWrappers = []struct {
	Name string
	Wrap func(tx pgx.Tx) sql.Tx
}{
	{
		Name: "TxFromPgx",
		Wrap: sql.TxFromPgx,
	},
	{
		Name: "PgxTx literal",
		Wrap: func(tx pgx.Tx) sql.Tx {
			return sql.PgxTx{Tx: tx}
		},
	},
}

func TestTxFromPgx_Commit(t *testing.T) {
	t.Parallel()

	pool := newPgxPool(t)
	table := createPgxTestTable(t, pool)

	for _, wrapper := range pgxTxWrappers {
		t.Run(wrapper.Name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			id := watermill.NewUUID()

			pgxTx, err := pool.Begin(ctx)
			require.NoError(t, err)
			// Releases the connection if the wrapped transaction failed to finish; no-op otherwise.
			defer func() { _ = pgxTx.Rollback(ctx) }()

			tx := wrapper.Wrap(pgxTx)

			_, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (id) VALUES ($1)`, id)
			require.NoError(t, err)

			require.NotPanics(t, func() {
				err = tx.Commit()
			})
			require.NoError(t, err)

			assert.True(t, pgxTestRowExists(t, pool, table, id), "committed row should be visible outside the transaction")
		})
	}
}

func TestTxFromPgx_Rollback(t *testing.T) {
	t.Parallel()

	pool := newPgxPool(t)
	table := createPgxTestTable(t, pool)

	for _, wrapper := range pgxTxWrappers {
		t.Run(wrapper.Name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			id := watermill.NewUUID()

			pgxTx, err := pool.Begin(ctx)
			require.NoError(t, err)
			// Releases the connection if the wrapped transaction failed to finish; no-op otherwise.
			defer func() { _ = pgxTx.Rollback(ctx) }()

			tx := wrapper.Wrap(pgxTx)

			_, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (id) VALUES ($1)`, id)
			require.NoError(t, err)

			require.NotPanics(t, func() {
				err = tx.Rollback()
			})
			require.NoError(t, err)

			assert.False(t, pgxTestRowExists(t, pool, table, id), "rolled back row should not be visible")
		})
	}
}

func newPgxPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	pool, err := pgxpool.New(context.Background(), connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	err = pool.Ping(context.Background())
	require.NoError(t, err)

	return pool
}

func createPgxTestTable(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	table := fmt.Sprintf(`"pgx_tx_test_%s"`, watermill.NewShortUUID())

	_, err := pool.Exec(context.Background(), `CREATE TABLE `+table+` (id VARCHAR(36) NOT NULL)`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+table)
		assert.NoError(t, err)
	})

	return table
}

func pgxTestRowExists(t *testing.T, pool *pgxpool.Pool, table string, id string) bool {
	t.Helper()

	var exists bool
	err := pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM `+table+` WHERE id = $1)`, id).Scan(&exists)
	require.NoError(t, err)

	return exists
}

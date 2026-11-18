package sql_test

import (
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
)

func TestPostgreSQLQueueOffsetsAdapter_AckMessageQuery(t *testing.T) {
	rows := []sql.Row{
		{Offset: 10},
		{Offset: 20},
	}

	t.Run("delete_on_ack_true", func(t *testing.T) {
		adapter := sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic:   "test_topic",
			LastRow: rows[1],
			Rows:    rows,
		})
		require.NoError(t, err)
		assert.Equal(t, `DELETE FROM "watermill_test_topic" WHERE "offset" = ANY($1)`, query.Query)
		require.Len(t, query.Args, 1)
		assert.Equal(t, pq.Array([]int64{10, 20}), query.Args[0])
	})

	t.Run("delete_on_ack_false", func(t *testing.T) {
		adapter := sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: false}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic:   "test_topic",
			LastRow: rows[1],
			Rows:    rows,
		})
		require.NoError(t, err)
		assert.Equal(t, `UPDATE "watermill_test_topic" SET acked = TRUE WHERE "offset" = ANY($1)`, query.Query)
		require.Len(t, query.Args, 1)
		assert.Equal(t, pq.Array([]int64{10, 20}), query.Args[0])
	})

	t.Run("empty_rows", func(t *testing.T) {
		adapter := sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic: "test_topic",
			Rows:  []sql.Row{},
		})
		require.NoError(t, err)
		assert.True(t, query.IsZero())
	})

	t.Run("consumer_group_panic", func(t *testing.T) {
		adapter := sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}
		assert.Panics(t, func() {
			_, _ = adapter.AckMessageQuery(sql.AckMessageQueryParams{
				Topic:         "test_topic",
				ConsumerGroup: "cg1",
				Rows:          rows,
			})
		})
	})
}

func TestMySQLQueueOffsetsAdapter_AckMessageQuery(t *testing.T) {
	rows := []sql.Row{
		{Offset: 10},
		{Offset: 20},
	}

	t.Run("delete_on_ack_true", func(t *testing.T) {
		adapter := sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic:   "test_topic",
			LastRow: rows[1],
			Rows:    rows,
		})
		require.NoError(t, err)
		assert.Equal(t, "DELETE FROM `watermill_test_topic` WHERE `offset` IN (?,?)", query.Query)
		require.Len(t, query.Args, 2)
		assert.Equal(t, []any{int64(10), int64(20)}, query.Args)
	})

	t.Run("delete_on_ack_false", func(t *testing.T) {
		adapter := sql.MySQLQueueOffsetsAdapter{DeleteOnAck: false}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic:   "test_topic",
			LastRow: rows[1],
			Rows:    rows,
		})
		require.NoError(t, err)
		assert.Equal(t, "UPDATE `watermill_test_topic` SET acked = TRUE WHERE `offset` IN (?,?)", query.Query)
		require.Len(t, query.Args, 2)
		assert.Equal(t, []any{int64(10), int64(20)}, query.Args)
	})

	t.Run("empty_rows", func(t *testing.T) {
		adapter := sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true}
		query, err := adapter.AckMessageQuery(sql.AckMessageQueryParams{
			Topic: "test_topic",
			Rows:  []sql.Row{},
		})
		require.NoError(t, err)
		assert.True(t, query.IsZero())
	})

	t.Run("consumer_group_panic", func(t *testing.T) {
		adapter := sql.MySQLQueueOffsetsAdapter{DeleteOnAck: true}
		assert.Panics(t, func() {
			_, _ = adapter.AckMessageQuery(sql.AckMessageQueryParams{
				Topic:         "test_topic",
				ConsumerGroup: "cg1",
				Rows:          rows,
			})
		})
	})
}

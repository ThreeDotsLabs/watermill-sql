package sql_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestConditionalPostgreSQLSchemaAdapter(t *testing.T) {
	db := newPostgreSQL(t)

	schemaAdapter := sql.ConditionalPostgreSQLSchema{
		GenerateWhereClause: func(params sql.GenerateWhereClauseParams) (string, []any) {
			return "(metadata->>'skip') IS NULL OR (metadata->>'skip') != 'true'", nil
		},
	}

	pub, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        schemaAdapter,
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	sub, err := sql.NewSubscriber(db, sql.SubscriberConfig{
		SchemaAdapter: schemaAdapter,
		OffsetsAdapter: sql.ConditionalPostgreSQLOffsetsAdapter{
			DeleteOnAck: true,
		},
		InitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topic := watermill.NewUUID()

	messages, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		msg := message.NewMessage(fmt.Sprint(i), []byte("{}"))
		if i%2 != 0 {
			msg.Metadata.Set("skip", "true")
		}
		err = pub.Publish(topic, msg)
		require.NoError(t, err)
	}

	var receivedMessages []*message.Message
	for i := 0; i < 5; i++ {
		select {
		case msg := <-messages:
			receivedMessages = append(receivedMessages, msg)
			msg.Ack()
		case <-time.After(100 * time.Millisecond):
			t.Errorf("expected to receive message")
			break
		}
	}

	require.Len(t, receivedMessages, 5)

	for _, msg := range receivedMessages {
		assert.NotEqual(t, "true", msg.Metadata.Get("skip"))

		id, err := strconv.Atoi(msg.UUID)
		require.NoError(t, err)

		assert.Equal(t, id%2, 0)
	}
}

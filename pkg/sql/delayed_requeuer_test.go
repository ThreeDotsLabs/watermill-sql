package sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestPostgreSQLDelayedRequeuer(t *testing.T) {
	t.Parallel()

	db := newPostgreSQL(t)
	schemaAdapter := sql.DefaultPostgreSQLSchema{}
	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{}
	publisher, subscriber := newPubSub(t, db, "test", schemaAdapter, offsetsAdapter)

	topic := watermill.NewUUID()

	err := subscriber.(message.SubscribeInitializer).SubscribeInitialize(topic)
	require.NoError(t, err)

	delayedRequeuer, err := sql.NewPostgreSQLDelayedRequeuer(sql.DelayedRequeuerConfig{
		DB:           db,
		RequeueTopic: watermill.NewUUID(),
		Publisher:    publisher,
		Logger:       logger,
	})
	require.NoError(t, err)

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(delayedRequeuer.Middleware()...)

	var receivedMessages []string

	router.AddNoPublisherHandler(
		"test",
		topic,
		subscriber,
		func(msg *message.Message) error {
			payload := string(msg.Payload)
			if payload == `{"error":true}` {
				return fmt.Errorf("error")
			}

			receivedMessages = append(receivedMessages, msg.UUID)

			return nil
		},
	)

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()

	<-router.Running()

	go func() {
		err := delayedRequeuer.Run(context.Background())
		require.NoError(t, err)
	}()

	err = publisher.Publish(topic, message.NewMessage("1", []byte(`{}`)))
	require.NoError(t, err)

	err = publisher.Publish(topic, message.NewMessage("2", []byte(`{"error":true}`)))
	require.NoError(t, err)

	err = publisher.Publish(topic, message.NewMessage("3", []byte(`{}`)))
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, []string{"1", "3"}, receivedMessages)
	}, 1*time.Second, 100*time.Millisecond)
}

func TestMySQLDelayedRequeuer(t *testing.T) {
	t.Parallel()

	db := newMySQL(t)
	schemaAdapter := sql.DefaultMySQLSchema{}
	offsetsAdapter := sql.DefaultMySQLOffsetsAdapter{}
	publisher, subscriber := newPubSub(t, db, "test", schemaAdapter, offsetsAdapter)

	topic := watermill.NewUUID()

	err := subscriber.(message.SubscribeInitializer).SubscribeInitialize(topic)
	require.NoError(t, err)

	delayedRequeuer, err := sql.NewMySQLDelayedRequeuer(sql.DelayedRequeuerConfig{
		DB:           db,
		RequeueTopic: watermill.NewUUID(),
		Publisher:    publisher,
		Logger:       logger,
	})
	require.NoError(t, err)

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(delayedRequeuer.Middleware()...)

	var receivedMessages []string

	router.AddNoPublisherHandler(
		"test",
		topic,
		subscriber,
		func(msg *message.Message) error {
			payload := string(msg.Payload)
			// MySQL and PostgreSQL format JSON with spaces, so we need to check both variants
			if payload == `{"error":true}` || payload == `{"error": true}` {
				return fmt.Errorf("error")
			}

			receivedMessages = append(receivedMessages, msg.UUID)

			return nil
		},
	)

	go func() {
		err := router.Run(context.Background())
		require.NoError(t, err)
	}()

	<-router.Running()

	go func() {
		err := delayedRequeuer.Run(context.Background())
		require.NoError(t, err)
	}()

	err = publisher.Publish(topic, message.NewMessage("1", []byte(`{}`)))
	require.NoError(t, err)

	err = publisher.Publish(topic, message.NewMessage("2", []byte(`{"error":true}`)))
	require.NoError(t, err)

	err = publisher.Publish(topic, message.NewMessage("3", []byte(`{}`)))
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Equal(t, []string{"1", "3"}, receivedMessages)
	}, 1*time.Second, 100*time.Millisecond)
}

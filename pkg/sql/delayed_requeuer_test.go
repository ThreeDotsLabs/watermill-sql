package sql_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/requeuer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
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
		// Requeue after a second instead of the default 10 seconds, so the test can wait for the requeued message.
		DelayOnError: &middleware.DelayOnError{
			InitialInterval: time.Second,
			MaxInterval:     time.Second,
			Multiplier:      1,
		},
		Logger: logger,
	})
	require.NoError(t, err)

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(delayedRequeuer.Middleware()...)

	var (
		receivedMessagesLock sync.Mutex
		receivedMessages     []string
	)

	router.AddConsumerHandler(
		"test",
		topic,
		subscriber,
		func(msg *message.Message) error {
			payload := string(msg.Payload)
			// Only the first delivery fails: the requeuer counts retries in the metadata of the message it requeues.
			if payload == `{"error":true}` && msg.Metadata.Get(requeuer.RetriesKey) == "" {
				return fmt.Errorf("error")
			}

			receivedMessagesLock.Lock()
			receivedMessages = append(receivedMessages, msg.UUID)
			receivedMessagesLock.Unlock()

			return nil
		},
	)

	routerDone := runInBackground(t, router.Run)

	select {
	case <-router.Running():
	case <-routerDone:
		t.Fatal("router stopped before it started running")
	}

	// The requeuer's subscriber and the poison queue publisher may create the requeue topic's table at the same time,
	// so this depends on PostgreSQL queue schema initialization being safe under concurrency (fixed separately).
	runInBackground(t, delayedRequeuer.Run)

	// Published in one call, so all three are queued before "2" fails and its requeued copy is queued after "3".
	err = publisher.Publish(
		topic,
		message.NewMessage("1", []byte(`{}`)),
		message.NewMessage("2", []byte(`{"error":true}`)),
		message.NewMessage("3", []byte(`{}`)),
	)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		receivedMessagesLock.Lock()
		received := slices.Clone(receivedMessages)
		receivedMessagesLock.Unlock()

		assert.Equal(t, []string{"1", "3", "2"}, received)
	}, 15*time.Second, 100*time.Millisecond)
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
		// Requeue after a second instead of the default 10 seconds, so the test can wait for the requeued message.
		DelayOnError: &middleware.DelayOnError{
			InitialInterval: time.Second,
			MaxInterval:     time.Second,
			Multiplier:      1,
		},
		Logger: logger,
	})
	require.NoError(t, err)

	router := message.NewDefaultRouter(logger)
	router.AddMiddleware(delayedRequeuer.Middleware()...)

	var (
		receivedMessagesLock sync.Mutex
		receivedMessages     []string
	)

	router.AddConsumerHandler(
		"test",
		topic,
		subscriber,
		func(msg *message.Message) error {
			payload := string(msg.Payload)
			// MySQL and PostgreSQL format JSON with spaces, so we need to check both variants
			// Only the first delivery fails: the requeuer counts retries in the metadata of the message it requeues.
			if (payload == `{"error":true}` || payload == `{"error": true}`) && msg.Metadata.Get(requeuer.RetriesKey) == "" {
				return fmt.Errorf("error")
			}

			receivedMessagesLock.Lock()
			receivedMessages = append(receivedMessages, msg.UUID)
			receivedMessagesLock.Unlock()

			return nil
		},
	)

	routerDone := runInBackground(t, router.Run)

	select {
	case <-router.Running():
	case <-routerDone:
		t.Fatal("router stopped before it started running")
	}

	runInBackground(t, delayedRequeuer.Run)

	// Published in one call, so all three are queued before "2" fails and its requeued copy is queued after "3".
	err = publisher.Publish(
		topic,
		message.NewMessage("1", []byte(`{}`)),
		message.NewMessage("2", []byte(`{"error":true}`)),
		message.NewMessage("3", []byte(`{}`)),
	)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		receivedMessagesLock.Lock()
		received := slices.Clone(receivedMessages)
		receivedMessagesLock.Unlock()

		assert.Equal(t, []string{"1", "3", "2"}, received)
	}, 15*time.Second, 100*time.Millisecond)
}

// The cleanup waits for run to return, so its pollers do not keep querying the database for the rest of the test
// binary. The error is checked with assert, not require: FailNow must be called from the test goroutine.
func runInBackground(t *testing.T, run func(ctx context.Context) error) <-chan struct{} {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		assert.NoError(t, run(ctx))
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return done
}

package sql_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

type fakePublisherExecutor struct {
	execCalls atomic.Int64

	onExec func(call int64)
}

func (e *fakePublisherExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	call := e.execCalls.Add(1)
	if e.onExec != nil {
		e.onExec(call)
	}

	return nil, nil
}

func (e *fakePublisherExecutor) QueryContext(ctx context.Context, query string, args ...any) (sql.Rows, error) {
	return nil, errors.New("QueryContext is not supported by fakePublisherExecutor")
}

func newFakeExecutorPublisher(t *testing.T, executor *fakePublisherExecutor) *sql.Publisher {
	t.Helper()

	publisher, err := sql.NewPublisher(executor, sql.PublisherConfig{
		SchemaAdapter: sql.DefaultPostgreSQLSchema{},
	}, logger)
	require.NoError(t, err)

	return publisher
}

func newTestMessage() *message.Message {
	return message.NewMessage(watermill.NewUUID(), []byte("{}"))
}

func TestPublisher_Publish_concurrent_with_Close(t *testing.T) {
	t.Parallel()

	const iterations = 50
	const publishingGoroutines = 10
	const closingGoroutines = 2

	topic := "test_" + watermill.NewUUID()

	for i := 0; i < iterations; i++ {
		var closeReturned, execAfterCloseReturned atomic.Bool
		executor := &fakePublisherExecutor{
			onExec: func(int64) {
				if closeReturned.Load() {
					execAfterCloseReturned.Store(true)
				}
			},
		}
		publisher := newFakeExecutorPublisher(t, executor)

		start := make(chan struct{})
		publishErrs := make(chan error, publishingGoroutines)
		closeErrs := make(chan error, closingGoroutines)
		wg := sync.WaitGroup{}

		for range publishingGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				publishErrs <- publisher.Publish(topic, newTestMessage())
			}()
		}

		for range closingGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := publisher.Close()
				closeReturned.Store(true)
				closeErrs <- err
			}()
		}

		close(start)
		wg.Wait()
		close(publishErrs)
		close(closeErrs)

		for err := range publishErrs {
			if err != nil {
				require.ErrorIs(t, err, sql.ErrPublisherClosed)
			}
		}
		for err := range closeErrs {
			require.NoError(t, err)
		}

		require.False(t, execAfterCloseReturned.Load(), "a Publish call reached the database after Close returned")
		require.ErrorIs(t, publisher.Publish(topic, newTestMessage()), sql.ErrPublisherClosed)
	}
}

func TestPublisher_Close_waits_for_in_flight_Publish(t *testing.T) {
	t.Parallel()

	topic := "test_" + watermill.NewUUID()

	firstExecStarted := make(chan struct{})
	releaseFirstExec := make(chan struct{})
	executor := &fakePublisherExecutor{
		onExec: func(call int64) {
			if call == 1 {
				close(firstExecStarted)
				<-releaseFirstExec
			}
		},
	}
	publisher := newFakeExecutorPublisher(t, executor)

	inFlightPublishErr := make(chan error, 1)
	go func() {
		inFlightPublishErr <- publisher.Publish(topic, newTestMessage())
	}()

	select {
	case <-firstExecStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("Publish did not reach the executor")
	}

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- publisher.Close()
	}()

	// Eventually, because the Close goroutine may not have marked the publisher closed yet.
	require.Eventually(t, func() bool {
		return errors.Is(publisher.Publish(topic, newTestMessage()), sql.ErrPublisherClosed)
	}, 30*time.Second, time.Millisecond)

	select {
	case <-closeErr:
		t.Fatal("Close returned while a Publish call was still in flight")
	default:
	}

	close(releaseFirstExec)

	select {
	case err := <-inFlightPublishErr:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("in-flight Publish did not return")
	}

	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return after the in-flight Publish returned")
	}

	execCallsBeforePublishAfterClose := executor.execCalls.Load()

	err := publisher.Publish(topic, newTestMessage())
	require.ErrorIs(t, err, sql.ErrPublisherClosed)

	assert.Equal(t, execCallsBeforePublishAfterClose, executor.execCalls.Load(),
		"Publish after Close must not reach the database")
}

func TestPublisher_Publish_without_messages_executes_nothing(t *testing.T) {
	t.Parallel()

	executor := &fakePublisherExecutor{}
	publisher, err := sql.NewPublisher(executor, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	err = publisher.Publish("test_" + watermill.NewUUID())
	require.NoError(t, err)

	assert.Equal(t, int64(0), executor.execCalls.Load(), "no schema initialization or insert query expected")
}

func TestPublisher_Publish_without_messages_validates_topic(t *testing.T) {
	t.Parallel()

	publisher := newFakeExecutorPublisher(t, &fakePublisherExecutor{})

	err := publisher.Publish("some_topic; DROP DATABASE `watermill`")
	require.ErrorIs(t, err, sql.ErrInvalidTopicName)
}

func TestPublisher_Publish_without_messages_after_Close(t *testing.T) {
	t.Parallel()

	publisher := newFakeExecutorPublisher(t, &fakePublisherExecutor{})
	require.NoError(t, publisher.Close())

	err := publisher.Publish("test_" + watermill.NewUUID())
	require.ErrorIs(t, err, sql.ErrPublisherClosed)
}

func TestPublisher_Publish_without_messages_on_database(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name          string
		DbConstructor func(t *testing.T) sql.Beginner
		SchemaAdapter sql.SchemaAdapter
	}{
		{
			Name:          "mysql",
			DbConstructor: newMySQL,
			SchemaAdapter: sql.DefaultMySQLSchema{},
		},
		{
			Name:          "mysql_queue",
			DbConstructor: newMySQL,
			SchemaAdapter: sql.MySQLQueueSchema{},
		},
		{
			Name:          "postgresql",
			DbConstructor: newPostgreSQL,
			SchemaAdapter: sql.DefaultPostgreSQLSchema{},
		},
		{
			Name:          "postgresql_queue",
			DbConstructor: newPostgreSQL,
			SchemaAdapter: sql.PostgreSQLQueueSchema{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			publisher, err := sql.NewPublisher(tc.DbConstructor(t), sql.PublisherConfig{
				SchemaAdapter:        tc.SchemaAdapter,
				AutoInitializeSchema: true,
			}, logger)
			require.NoError(t, err)

			err = publisher.Publish("test_" + watermill.NewUUID())
			require.NoError(t, err)
		})
	}
}

func TestPublisher_concurrent_Close_calls(t *testing.T) {
	t.Parallel()

	const iterations = 100

	for i := 0; i < iterations; i++ {
		publisher := newFakeExecutorPublisher(t, &fakePublisherExecutor{})

		start := make(chan struct{})
		closeErrs := make(chan error, 2)

		for range 2 {
			go func() {
				<-start
				closeErrs <- publisher.Close()
			}()
		}

		close(start)

		require.NoError(t, <-closeErrs)
		require.NoError(t, <-closeErrs)
	}
}

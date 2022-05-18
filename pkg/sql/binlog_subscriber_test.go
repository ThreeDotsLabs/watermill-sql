package sql_test

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

const (
	waitTime = time.Second * 100000
)

var (
	dbConfig = sql.Database{
		Host:     "127.0.0.1",
		Port:     "3306",
		User:     "root",
		Password: "",
		Name:     "watermill",
		Flavour:  "mysql",
	}
)

func TestBinlogSubscriber_Subscribe_multiple_consumer_groups(t *testing.T) {
	t.Parallel()

	db := newMySQL(t)
	firstConsumerGroupName := "first_consumer_group_" + watermill.NewULID()
	secondConsumerGroupName := "second_consumer_group_" + watermill.NewULID()

	topic := "test_" + watermill.NewULID()

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	// publish first batch the messages
	firstMessagesBatch := getTestMessages(100)
	for _, msg := range firstMessagesBatch {
		err = publisher.Publish(topic, msg)
		require.NoError(t, err)
	}

	t.Run("first consumer group should receive first batch of the messages", func(t *testing.T) {
		firstSubscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    firstConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLBinlogOffsetsAdapter{},
			InitializeSchema: true,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messageStream, err := firstSubscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		actualMessages := waitForMessages(t, len(firstMessagesBatch), messageStream)

		assertMessages(t, firstMessagesBatch, actualMessages)

		err = firstSubscriber.Close()
		require.NoError(t, err)

		assertMessageChannelClosed(t, messageStream)
	})

	// publish second batch the messages
	secondMessagesBatch := getTestMessages(100)
	err = publisher.Publish(topic, secondMessagesBatch...)
	require.NoError(t, err)

	t.Run("first consumer group should receive second batch of the messages (skipping already acked messages)", func(t *testing.T) {
		subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    firstConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLBinlogOffsetsAdapter{},
			InitializeSchema: false,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messageStream, err := subscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		actualMessages := waitForMessages(t, len(secondMessagesBatch), messageStream)

		assertMessages(t, secondMessagesBatch, actualMessages)

		err = subscriber.Close()
		require.NoError(t, err)

		assertMessageChannelClosed(t, messageStream)
	})

	t.Run("second consumer group should receive all messages from both batches and in-order", func(t *testing.T) {
		subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    secondConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLBinlogOffsetsAdapter{},
			InitializeSchema: false,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messageStream, err := subscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		expectedMessages := append(firstMessagesBatch, secondMessagesBatch...)
		actualMessages := waitForMessages(t, len(expectedMessages), messageStream)

		assertMessages(t, expectedMessages, actualMessages)

		err = subscriber.Close()
		require.NoError(t, err)

		assertMessageChannelClosed(t, messageStream)
	})
}

func TestBinlogSubscriber_Subscribe_multiple_subscriptions_concurrently(t *testing.T) {
	t.Parallel()

	db := newMySQL(t)
	consumerGroup := "consumer_group_" + watermill.NewULID()
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topics := map[string][]*message.Message{
		"first_" + watermill.NewULID():  getTestMessages(12),
		"second_" + watermill.NewULID(): getTestMessages(32),
		"third_" + watermill.NewULID():  getTestMessages(21),
	}

	for topic, messages := range topics {
		err = publisher.Publish(topic, messages...)
		require.NoError(t, err)

		// force creation of new binlog log file
		_, err := db.Exec("FLUSH LOGS")
		require.NoError(t, err)
	}

	subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
		ConsumerGroup:    consumerGroup,
		SchemaAdapter:    sql.DefaultMySQLSchema{},
		OffsetsAdapter:   sql.DefaultMySQLBinlogOffsetsAdapter{},
		InitializeSchema: true,
		Database:         dbConfig,
	}, logger)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(len(topics))
	for topic, expectedMessages := range topics {
		topic := topic
		go func(messages []*message.Message) {
			messageStream, err := subscriber.Subscribe(context.Background(), topic)
			require.NoError(t, err)

			actualMessages := waitForMessages(t, len(messages), messageStream)

			assertMessages(t, messages, actualMessages)
			wg.Done()
		}(expectedMessages)
	}

	wg.Wait()

	err = subscriber.Close()
	require.NoError(t, err)
}

func waitForMessages(t *testing.T, expectedNumberOfMessages int, messages <-chan *message.Message) []*message.Message {
	t.Helper()

	actualMessages := []*message.Message{}

	for i := 0; i < expectedNumberOfMessages; i++ {
		select {
		case received, ok := <-messages:
			if !ok {
				t.Fatalf("Message channel closed unexpectedly")
			}
			received.Ack()
			actualMessages = append(actualMessages, received)
		case <-time.After(waitTime):
			t.Fatalf("Didn't receive expected number of messages")
		}
	}

	return actualMessages
}

func getTestMessages(n int) []*message.Message {
	msgs := []*message.Message{}

	for i := 0; i < n; i++ {
		payload := fmt.Sprintf(`{"id":"%d","value":"%s"}`, i, watermill.NewULID())

		msg := message.NewMessage(watermill.NewULID(), []byte(payload))
		msg.Metadata.Set(watermill.NewULID(), watermill.NewULID())

		msgs = append(msgs, msg)
	}

	return msgs
}

func assertMessages(t *testing.T, expected []*message.Message, actual []*message.Message) {
	require.Equal(t, len(expected), len(actual), "messages count needs to be the same")

	for i, expectedMsg := range expected {
		require.Equal(t, string(expectedMsg.Payload), string(actual[i].Payload))
		require.True(t, expectedMsg.Equals(actual[i]))
	}
}

func assertMessageChannelClosed(t *testing.T, msgStream <-chan *message.Message) {
	_, isOpen := <-msgStream
	require.False(t, isOpen, "chanel should be closed")
}

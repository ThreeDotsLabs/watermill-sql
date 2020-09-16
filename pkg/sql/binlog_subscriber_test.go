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
	waitTime = time.Second
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

func TestBinlogSubscriber(t *testing.T) {
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
	firstMessagesBatch := getTestMessages(11)
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
	secondMessagesBatch := getTestMessages(13)
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

	t.Run("second consumer group should receive all messages from both batches and in order", func(t *testing.T) {
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

func TestBinlogSubscriber_multiple_subscribers(t *testing.T) {
	db := newMySQL(t)
	consumerGroup := "consumer_group_" + watermill.NewULID()
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topics := map[string][]*message.Message{
		"first_" + watermill.NewULID():  getTestMessages(7),
		"second_" + watermill.NewULID(): getTestMessages(11),
		"third_" + watermill.NewULID():  getTestMessages(13),
	}

	var wg sync.WaitGroup

	for topic, messages := range topics {
		wg.Add(1)
		go func(topic string, messages []*message.Message) {
			err = publisher.Publish(topic, messages...)
			require.NoError(t, err)

			wg.Done()
		}(topic, messages)
	}

	wg.Wait()

	subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
		ConsumerGroup:    consumerGroup,
		SchemaAdapter:    sql.DefaultMySQLSchema{},
		OffsetsAdapter:   sql.DefaultMySQLBinlogOffsetsAdapter{},
		InitializeSchema: true,
		Database:         dbConfig,
	}, logger)
	require.NoError(t, err)

	for topic, expectedMessages := range topics {
		wg.Add(1)
		messageStream, err := subscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)
		go func(messages []*message.Message) {
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
	actualMessages := []*message.Message{}

	for i := 0; i < expectedNumberOfMessages; i++ {
		select {
		case received := <-messages:
			received.Ack()
			actualMessages = append(actualMessages, received)
		case <-time.After(waitTime):
			t.Error("Didn't receive any messages")
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

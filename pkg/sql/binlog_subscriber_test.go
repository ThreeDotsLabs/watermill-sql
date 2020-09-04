package sql_test

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	firstConsumerGroupName  = "first_consumer_group"
	secondConsumerGroupName = "second_consumer_group"
	waitTime                = time.Second * 1000
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
	topic := "test_" + watermill.NewULID()

	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	// publish first batch the messages
	firstMessagesBatch := getTestMessages(11)
	err = publisher.Publish(topic, firstMessagesBatch...)
	require.NoError(t, err)

	t.Run("first consumer group should receive first batch of the messages", func(t *testing.T) {
		firstSubscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    firstConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: true,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messagesStream, err := firstSubscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		actualMessages := waitForMessages(t, len(firstMessagesBatch), messagesStream)

		assertMessages(t, firstMessagesBatch, actualMessages)

		err = firstSubscriber.Close()
		require.NoError(t, err)
	})

	// publish second batch the messages
	secondMessagesBatch := getTestMessages(13)
	err = publisher.Publish(topic, secondMessagesBatch...)
	require.NoError(t, err)

	t.Run("first consumer group should receive second batch of the messages (skipping already acked messages)", func(t *testing.T) {
		subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    firstConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: false,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messagesStream, err := subscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		actualMessages := waitForMessages(t, len(secondMessagesBatch), messagesStream)

		assertMessages(t, secondMessagesBatch, actualMessages)

		err = subscriber.Close()
		require.NoError(t, err)
	})

	t.Run("second consumer group should receive all messages from both batches and in order", func(t *testing.T) {
		subscriber, err := sql.NewBinlogSubscriber(db, sql.BinlogSubscriberConfig{
			ConsumerGroup:    secondConsumerGroupName,
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: false,
			Database:         dbConfig,
		}, logger)
		require.NoError(t, err)

		messagesStream, err := subscriber.Subscribe(context.Background(), topic)
		require.NoError(t, err)

		expectedMessages := append(firstMessagesBatch, secondMessagesBatch...)
		actualMessages := waitForMessages(t, len(expectedMessages), messagesStream)

		assertMessages(t, expectedMessages, actualMessages)

		err = subscriber.Close()
		require.NoError(t, err)
	})
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
	for i, expectedMsg := range expected {
		require.Equal(t, string(expectedMsg.Payload), string(actual[i].Payload))
		require.True(t, expectedMsg.Equals(actual[i]))
	}
}

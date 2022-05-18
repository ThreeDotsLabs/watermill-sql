package streamer_test

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/internal/streamer"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-mysql-org/go-mysql/mysql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	zeroPosition = mysql.Position{}

	testWait = time.Second * 5
	logger   = watermill.NewStdLogger(false, false)
)

type testPayload struct {
	ID    string
	Value int
}

func TestStreamer_Stream_all_messages_in_order(t *testing.T) {
	db := newMySQL(t)
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topicName := uuid.New().String()

	id := uuid.New().String()
	expectedMessages := make([]*message.Message, 0)
	for i := 0; i < 100; i++ {
		payload, err := json.Marshal(testPayload{
			ID:    id,
			Value: i,
		})
		require.NoError(t, err)

		msg := message.NewMessage(uuid.New().String(), payload)

		expectedMessages = append(expectedMessages, msg)
	}

	err = publisher.Publish(topicName, expectedMessages...)
	require.NoError(t, err)

	streamerConfig := newStreamerConfig(topicName)
	streamer, err := streamer.NewStreamer(streamerConfig, logger)
	require.NoError(t, err)

	actualMessages := make([]*message.Message, 0)
	out, err := streamer.Stream(context.Background(), zeroPosition)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case row := <-out:
				p := testPayload{}
				err := json.Unmarshal(row.Payload(), &p)
				require.NoError(t, err)

				if p.ID == id {
					msg, err := row.ToMessage()
					require.NoError(t, err)

					actualMessages = append(actualMessages, msg)
				}
			case <-time.After(testWait):
				return
			}
		}
	}()

	wg.Wait()

	require.Equal(t, len(expectedMessages), len(actualMessages))
	for i, expectedMessage := range expectedMessages {
		require.True(t, expectedMessage.Equals(actualMessages[i]), "messages should be the same")
	}
}

func TestStreamer_Stream_cancel_by_context(t *testing.T) {
	db := newMySQL(t)
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topicName := uuid.New().String()
	msg := message.NewMessage(uuid.New().String(), nil)
	err = publisher.Publish(topicName, msg)
	require.NoError(t, err)

	streamerConfig := newStreamerConfig(topicName)
	streamer, err := streamer.NewStreamer(streamerConfig, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	out, err := streamer.Stream(ctx, zeroPosition)
	require.NoError(t, err)

	cancel()
	_, ok := <-out
	require.False(t, ok, "output channel must be closed after context cancelled")
}

func TestStreamer_Stream_cancel_called(t *testing.T) {
	db := newMySQL(t)
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topicName := uuid.New().String()
	msg := message.NewMessage(uuid.New().String(), nil)
	err = publisher.Publish(topicName, msg)
	require.NoError(t, err)

	streamerConfig := newStreamerConfig(topicName)
	s, err := streamer.NewStreamer(streamerConfig, logger)
	require.NoError(t, err)

	//TODO: add test case for cancel with context
	out, err := s.Stream(context.Background(), zeroPosition)
	require.NoError(t, err)

	s.Close()

	_, ok := <-out
	require.False(t, ok, "output channel must be closed after Cancel called")
}

func TestStreamer_Stream_restart_with_returned_position(t *testing.T) {
	db := newMySQL(t)
	publisher, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, logger)
	require.NoError(t, err)

	topicName := uuid.New().String()

	expectedMessages := make([]*message.Message, 0)
	for i := 0; i < 2; i++ {
		msg := message.NewMessage(uuid.New().String(), nil)

		err = publisher.Publish(topicName, msg)
		require.NoError(t, err)

		expectedMessages = append(expectedMessages, msg)
	}

	streamerConfig := newStreamerConfig(topicName)
	s, err := streamer.NewStreamer(streamerConfig, logger)
	require.NoError(t, err)

	out, err := s.Stream(context.Background(), zeroPosition)
	require.NoError(t, err)

	actualMessages := make([]*message.Message, 0)
	lastReceivedPosition := mysql.Position{}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case row := <-out:
				msg, err := row.ToMessage()
				require.NoError(t, err)
				actualMessages = append(actualMessages, msg)

				lastReceivedPosition = row.Position()
			case <-time.After(testWait):
				return
			}
		}
	}()

	wg.Wait()

	s.Close()

	for i := 0; i < 2; i++ {
		msg := message.NewMessage(uuid.New().String(), nil)

		err = publisher.Publish(topicName, msg)
		require.NoError(t, err)

		expectedMessages = append(expectedMessages, msg)
	}

	s, err = streamer.NewStreamer(streamerConfig, logger)
	require.NoError(t, err)

	out, err = s.Stream(context.Background(), lastReceivedPosition)
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case row := <-out:
				msg, err := row.ToMessage()
				require.NoError(t, err)
				actualMessages = append(actualMessages, msg)
			case <-time.After(testWait):
				return
			}
		}
	}()

	wg.Wait()

	require.Len(t, actualMessages, 5)
	require.True(t, expectedMessages[0].Equals(actualMessages[0]), "messages should be the same")
	require.True(t, expectedMessages[1].Equals(actualMessages[1]), "messages should be the same")
	// Duplication of messages here stems from the position of binlog that I am saving
	// I am saving the last saved position before a message is read.
	// This might be solved by an upstream Subscriber which will have to verify messages against offset
	require.Len(t, actualMessages, 5)
	require.True(t, expectedMessages[1].Equals(actualMessages[2]), "messages should be the same")
	require.True(t, expectedMessages[2].Equals(actualMessages[3]), "messages should be the same")
	require.True(t, expectedMessages[3].Equals(actualMessages[4]), "messages should be the same")
}

func newStreamerConfig(topic string) streamer.StreamsConfig {
	return streamer.StreamsConfig{
		Table: fmt.Sprintf("watermill_%s", topic),
		Database: streamer.Database{
			Host:     "localhost",
			Port:     "3306",
			User:     "root",
			Password: "",
			Name:     "watermill",
			Flavour:  "",
		},
		ColumnsMapping: streamer.ColumnsMapping{
			UUID:     "uuid",
			Offset:   "offset",
			Payload:  "payload",
			Metadata: "metadata",
		},
	}
}

func newMySQL(t *testing.T) *stdSQL.DB {
	addr := os.Getenv("WATERMILL_TEST_MYSQL_HOST")
	if addr == "" {
		addr = "localhost"
	}
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = addr

	conf.DBName = "watermill"

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

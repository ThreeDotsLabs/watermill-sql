package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, false)
)

func newPubSub(t *testing.T, db *stdSQL.DB, consumerGroup string, schemaAdapter sql.SchemaAdapter, offsetsAdapter sql.OffsetsAdapter) (message.Publisher, message.Subscriber) {
	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			ConsumerGroup: consumerGroup,

			PollInterval:   100 * time.Millisecond,
			ResendInterval: 50 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
			OffsetsAdapter: offsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
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

func newPostgreSQL(t *testing.T) *stdSQL.DB {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	db, err := stdSQL.Open("postgres", connStr)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func newPgxPostgreSQL(t *testing.T) *stdSQL.DB {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	conf, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)

	db := stdlib.OpenDB(*conf)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

func createMySQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &testMySQLSchema{
		sql.DefaultMySQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf("`test_%s`", topic)
			},
		},
	}

	offsetsAdapter := sql.DefaultMySQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf("`test_offsets_%s`", topic)
		},
	}

	return newPubSub(t, newMySQL(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createMySQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createMySQLPubSubWithConsumerGroup(t, "test")
}

func createPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &testPostgreSQLSchema{
		sql.DefaultPostgreSQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf(`"test_%s"`, topic)
			},
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPostgreSQL(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPgxPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &testPostgreSQLSchema{
		sql.DefaultPostgreSQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf(`"test_%s"`, topic)
			},
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPgxPostgreSQL(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func TestMySQLPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createMySQLPubSub,
		createMySQLPubSubWithConsumerGroup,
	)
}

func TestPostgreSQLPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPostgreSQLPubSub,
		createPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestPgxPostgreSQLPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPgxPostgreSQLPubSub,
		createPgxPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestCtxValues(t *testing.T) {
	pubSubConstructors := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
	}{
		{
			Name:        "mysql",
			Constructor: createMySQLPubSub,
		},
		{
			Name:        "postgresql",
			Constructor: createPostgreSQLPubSub,
		},
	}

	for _, constructor := range pubSubConstructors {
		pub, sub := constructor.Constructor(t)

		t.Run("", func(t *testing.T) {
			t.Parallel()
			topicName := "topic_" + watermill.NewUUID()

			err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
			require.NoError(t, err)

			var messagesToPublish []*message.Message

			id := watermill.NewUUID()
			messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

			err = pub.Publish(topicName, messagesToPublish...)
			require.NoError(t, err, "cannot publish message")

			messages, err := sub.Subscribe(context.Background(), topicName)
			require.NoError(t, err)

			select {
			case msg := <-messages:
				tx, ok := sql.TxFromContext(msg.Context())
				assert.True(t, ok)
				assert.NotNil(t, t, tx)
				assert.IsType(t, &stdSQL.Tx{}, tx)
			case <-time.After(time.Second * 10):
				t.Fatal("no message received")
			}
		})
	}
}

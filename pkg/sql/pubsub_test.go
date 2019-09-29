package sql_test

import (
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
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, true)
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

func createPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLPubSubWithConsumerGroup(t, "test")
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

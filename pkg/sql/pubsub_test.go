package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wpgx "github.com/ThreeDotsLabs/watermill-sql/v3/pkg/pgx"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

func newPubSub(t *testing.T, db sql.Beginner, consumerGroup string, schemaAdapter sql.SchemaAdapter, offsetsAdapter sql.OffsetsAdapter) (message.Publisher, message.Subscriber) {
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

			PollInterval:   1 * time.Millisecond,
			ResendInterval: 5 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
			OffsetsAdapter: offsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func newMySQL(t *testing.T) sql.Beginner {
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

	return sql.StdSQLBeginner{DB: db}
}

func newPostgreSQL(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	db, err := stdSQL.Open("postgres", connStr)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return sql.StdSQLBeginner{DB: db}
}

func newPgxPostgreSQL(t *testing.T) sql.Beginner {
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

	return sql.StdSQLBeginner{DB: db}
}

func newPgx(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)

	db, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)

	err = db.Ping(context.Background())
	require.NoError(t, err)

	return wpgx.Beginner{Conn: db}
}

func createMySQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		newMySQL(t),
		consumerGroup,
		newMySQLSchemaAdapter(0),
		newMySQLOffsetsAdapter(),
	)
}

func newMySQLOffsetsAdapter() sql.DefaultMySQLOffsetsAdapter {
	return sql.DefaultMySQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf("`test_offsets_%s`", topic)
		},
	}
}

func newMySQLSchemaAdapter(batchSize int) *testMySQLSchema {
	return &testMySQLSchema{
		sql.DefaultMySQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf("`test_%s`", topic)
			},
			SubscribeBatchSize: batchSize,
		},
	}
}

func createMySQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createMySQLPubSubWithConsumerGroup(t, "test")
}

func createPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		newPostgreSQL(t),
		consumerGroup,
		newPostgresSchemaAdapter(0),
		newPostgresOffsetsAdapter(),
	)
}

func newPostgresOffsetsAdapter() sql.DefaultPostgreSQLOffsetsAdapter {
	return sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}
}

func newPostgresSchemaAdapter(batchSize int) *testPostgreSQLSchema {
	return &testPostgreSQLSchema{
		sql.DefaultPostgreSQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf(`"test_%s"`, topic)
			},
			SubscribeBatchSize: batchSize,
		},
	}
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

func createPgxPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &testPostgreSQLSchema{
		sql.DefaultPostgreSQLSchema{
			GenerateMessagesTableName: func(topic string) string {
				return fmt.Sprintf(`"test_pgx_%s"`, topic)
			},
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_pgx_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPgx(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPubSubWithConsumerGroup(t, "test")
}

func TestMySQLPublishSubscribe(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
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

func TestPgxPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPgxPubSub,
		createPgxPubSubWithConsumerGroup,
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

		t.Run(constructor.Name, func(t *testing.T) {
			t.Parallel()
			topicName := "topic_" + watermill.NewUUID()

			err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
			require.NoError(t, err)

			var messagesToPublish []*message.Message

			id := watermill.NewUUID()
			messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

			err = pub.Publish(topicName, messagesToPublish...)
			require.NoError(t, err, "cannot publish message")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			messages, err := sub.Subscribe(ctx, topicName)
			require.NoError(t, err)

			select {
			case msg := <-messages:
				tx, ok := sql.TxFromContext(msg.Context())
				assert.True(t, ok)
				assert.NotNil(t, t, tx)
				assert.IsType(t, &sql.StdSQLTx{}, tx)
				msg.Ack()
			case <-time.After(time.Second * 10):
				t.Fatal("no message received")
			}
		})
	}
}

// TestNotMissingMessages checks if messages are not missing when messages are published in concurrent transactions.
// See more: https://github.com/ThreeDotsLabs/watermill/issues/311
func TestNotMissingMessages(t *testing.T) {
	t.Parallel()

	pubSubs := []struct {
		Name           string
		DbConstructor  func(t *testing.T) sql.Beginner
		SchemaAdapter  sql.SchemaAdapter
		OffsetsAdapter sql.OffsetsAdapter
	}{
		{
			Name:           "mysql",
			DbConstructor:  newMySQL,
			SchemaAdapter:  newMySQLSchemaAdapter(0),
			OffsetsAdapter: newMySQLOffsetsAdapter(),
		},
		{
			Name:           "postgresql",
			DbConstructor:  newPostgreSQL,
			SchemaAdapter:  newPostgresSchemaAdapter(0),
			OffsetsAdapter: newPostgresOffsetsAdapter(),
		},
	}

	for _, pubSub := range pubSubs {
		pubSub := pubSub

		t.Run(pubSub.Name, func(t *testing.T) {
			t.Parallel()

			db := pubSub.DbConstructor(t)

			topicName := "topic_" + watermill.NewUUID()

			messagesToPublish := []*message.Message{
				message.NewMessage("0", nil),
				message.NewMessage("1", nil),
				message.NewMessage("2", nil),
			}

			sub, err := sql.NewSubscriber(
				db,
				sql.SubscriberConfig{
					ConsumerGroup: "consumerGroup",

					PollInterval:   1 * time.Millisecond,
					ResendInterval: 5 * time.Millisecond,
					SchemaAdapter:  pubSub.SchemaAdapter,
					OffsetsAdapter: pubSub.OffsetsAdapter,
				},
				logger,
			)
			require.NoError(t, err)

			err = sub.SubscribeInitialize(topicName)
			require.NoError(t, err)

			messagesAsserted := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer close(messagesAsserted)

				messages, err := sub.Subscribe(ctx, topicName)
				require.NoError(t, err)

				received, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
				assert.True(t, all)

				tests.AssertAllMessagesReceived(t, messagesToPublish, received)
			}()

			tx0, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx1, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			txRollback, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx2, err := db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			pub0, err := sql.NewPublisher(
				tx0,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub0.Publish(topicName, messagesToPublish[0])
			require.NoError(t, err, "cannot publish message")

			pub1, err := sql.NewPublisher(
				tx1,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub1.Publish(topicName, messagesToPublish[1])
			require.NoError(t, err, "cannot publish message")

			pubRollback, err := sql.NewPublisher(
				txRollback,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pubRollback.Publish(topicName, message.NewMessage("rollback", nil))
			require.NoError(t, err, "cannot publish message")

			pub2, err := sql.NewPublisher(
				tx2,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub2.Publish(topicName, messagesToPublish[2])
			require.NoError(t, err, "cannot publish message")

			require.NoError(t, tx2.Commit())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, txRollback.Rollback())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx1.Commit())
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx0.Commit())
			time.Sleep(time.Millisecond * 10)

			<-messagesAsserted
		})
	}
}

func TestConcurrentSubscribe_different_bulk_sizes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
		Test        func(t *testing.T, tCtx tests.TestContext, pubSubConstructor tests.PubSubConstructor)
	}{
		{
			Name: "TestPublishSubscribe_mysql_1",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newMySQL(t),
					"test",
					newMySQLSchemaAdapter(1),
					newMySQLOffsetsAdapter(),
				)
			},
			Test: tests.TestPublishSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_mysql_5",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newMySQL(t),
					"test",
					newMySQLSchemaAdapter(5),
					newMySQLOffsetsAdapter(),
				)
			},
			Test: tests.TestConcurrentSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_postgresql_1",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(1),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestPublishSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_postgresql_5",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(5),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestConcurrentSubscribe,
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			tc.Test(
				t,
				tests.TestContext{
					TestID: tests.NewTestID(),
					Features: tests.Features{
						ConsumerGroups:                      true,
						ExactlyOnceDelivery:                 true,
						GuaranteedOrder:                     true,
						GuaranteedOrderWithSingleSubscriber: true,
						Persistent:                          true,
					},
				},
				tc.Constructor,
			)
		})
	}
}

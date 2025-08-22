package sql_test

import (
	"context"
	stdSQL "database/sql"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(false, false)
)

func newPubSub(t *testing.T, beginner sql.Beginner, consumerGroup string, schemaAdapter sql.SchemaAdapter, offsetsAdapter sql.OffsetsAdapter) (message.Publisher, message.Subscriber) {
	publisher, err := sql.NewPublisher(
		beginner,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		beginner,
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

	return sql.BeginnerFromStdSQL(db)
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

	return sql.BeginnerFromStdSQL(db)
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

	return sql.BeginnerFromStdSQL(db)
}

func newPgx(t *testing.T) sql.Beginner {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	conf, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)

	db, err := pgxpool.NewWithConfig(context.Background(), conf)
	require.NoError(t, err)

	err = db.Ping(context.Background())
	require.NoError(t, err)

	return sql.BeginnerFromPgx(db)
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

func newMySQLSchemaAdapter(batchSize int) *sql.DefaultMySQLSchema {
	return &sql.DefaultMySQLSchema{
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf("`test_%s`", topic)
		},
		GeneratePayloadType: func(topic string) string {
			// payload is a VARBINARY(255) instead of JSON; tests don't presuppose JSON-marshallable payloads
			return "VARBINARY(255)"
		},
		SubscribeBatchSize: batchSize,
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

func newPostgresSchemaAdapter(batchSize int) *sql.DefaultPostgreSQLSchema {
	return &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
		SubscribeBatchSize: batchSize,
	}
}

func createPgxPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
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
	schemaAdapter := &sql.DefaultPostgreSQLSchema{
		InitializeSchemaLock: rand.Intn(1000000),
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_pgx_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
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

func createPostgreSQLQueue(t *testing.T, db sql.Beginner) (message.Publisher, message.Subscriber) {
	schemaAdapter := sql.PostgreSQLQueueSchema{
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
	}
	offsetsAdapter := sql.PostgreSQLQueueOffsetsAdapter{
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
	}

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

func TestPgxPostgreSQLQueue(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      false,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return createPostgreSQLQueue(t, newPgxPostgreSQL(t))
		},
		nil,
	)
}

func findRowsRemovedByFilterInAnalyze(input string) []int {
	pattern := `Rows Removed by Filter: (\d+)`
	re := regexp.MustCompile(pattern)

	matches := re.FindAllStringSubmatch(input, -1)

	result := make([]int, 0, len(matches))

	for _, match := range matches {
		if len(match) > 1 {
			value, err := strconv.Atoi(match[1])
			if err == nil {
				result = append(result, value)
			}
		}
	}

	return result
}

func extractDurationFromAnalyze(t *testing.T, s string) time.Duration {
	t.Helper()

	// Regular expression to match "Execution Time: X.XXX ms" pattern
	re := regexp.MustCompile(`Execution Time:\s*(\d+(?:\.\d+)?)\s*ms`)

	match := re.FindStringSubmatch(s)
	if len(match) != 2 {
		t.Fatalf("cannot find duration in the string: %s", s)
	}

	durationStr := match[1]

	parsed, err := time.ParseDuration(durationStr + "ms")
	require.NoError(t, err)

	return parsed
}

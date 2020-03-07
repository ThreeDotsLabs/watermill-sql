package sql_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/stretchr/testify/require"
)

func TestListenNotifyPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      false,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          false,
	}

	tests.TestPubSub(
		t,
		features,
		createListenNotifyPubSub,
		nil,
	)
	tests.TestPublishSubscribe(
		t,
		tests.TestContext{
			TestID:   "13515135313",
			Features: features,
		},
		createListenNotifyPubSub,
	)
}

func createListenNotifyPubSub(t *testing.T) (publisher message.Publisher, subscriber message.Subscriber) {
	db := newPostgreSQL(t)
	pub, err := sql.NewNotifyPostgresPublisher(
		db,
		sql.NotifyPostgresPublisherConfig{
			Marshaler: sql.DefaultPostgresNotifyMarshaler{},
		},
		logger,
	)
	require.NoError(t, err)

	sub, err := sql.NewPostgresListenSubscriber(
		sql.PostgresListenSubscriberConfig{
			ConnectionString:     postgreSQLConnString(),
			MinReconnectInterval: time.Millisecond,
			MaxReconnectInterval: time.Second,
			Unmarshaler:          sql.DefaultPostgresListenUnmarshaler{},
		},
		logger,
	)

	return pub, sub
}

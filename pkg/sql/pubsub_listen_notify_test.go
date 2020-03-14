package sql_test

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func createPostgreSQLListenNotifyPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLListenNotifyPubSubWithConsumerGroup(t, "test")
}

func createPostgreSQLListenNotifyPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
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

	return newPubSub(t, newPostgreSQL(t), consumerGroup, schemaAdapter, offsetsAdapter, true)
}

func TestPostgreSQLListenNotifyPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPostgreSQLListenNotifyPubSub,
		createPostgreSQLListenNotifyPubSubWithConsumerGroup,
	)
}

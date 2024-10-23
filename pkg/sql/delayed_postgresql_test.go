package sql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestDelayedPostgreSQL(t *testing.T) {
	t.Parallel()

	db := newPostgreSQL(t)

	pub, err := sql.NewDelayedPostgreSQLPublisher(db, sql.DelayedPostgreSQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			DefaultDelay: delay.For(150 * time.Millisecond),
		},
		Logger: logger,
	})
	require.NoError(t, err)

	sub, err := sql.NewDelayedPostgreSQLSubscriber(db, sql.DelayedPostgreSQLSubscriberConfig{
		DeleteOnAck: true,
		Logger:      logger,
	})
	require.NoError(t, err)

	topic := watermill.NewUUID()

	messages, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), []byte("{}"))

	err = pub.Publish(topic, msg)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		select {
		case <-messages:
			t.Errorf("message should not be received")
		default:
		}
	}, time.Millisecond*100, time.Millisecond*10)

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		select {
		case received := <-messages:
			assert.Equal(t, msg.UUID, received.UUID)
			received.Ack()
		default:
		}
	}, time.Millisecond*100, time.Millisecond*10)
}

func TestDelayedPostgreSQLNotStrict(t *testing.T) {
	t.Parallel()

	db := newPostgreSQL(t)

	pub, err := sql.NewDelayedPostgreSQLPublisher(db, sql.DelayedPostgreSQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			DefaultDelay: delay.For(150 * time.Millisecond),
		},
		Logger: logger,
	})
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), []byte("{}"))

	err = pub.Publish(watermill.NewUUID(), msg)
	require.Error(t, err)

	delay.Message(msg, delay.For(10*time.Second))
	err = pub.Publish(watermill.NewUUID(), msg)
	require.NoError(t, err)

}

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

func TestDelayedMySQL(t *testing.T) {
	t.Parallel()

	db := newMySQL(t)

	pub, err := sql.NewDelayedMySQLPublisher(db, sql.DelayedMySQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			DefaultDelayGenerator: func(params delay.DefaultDelayGeneratorParams) (delay.Delay, error) {
				return delay.For(time.Second), nil
			},
		},
		Logger: logger,
	})
	require.NoError(t, err)

	sub, err := sql.NewDelayedMySQLSubscriber(db, sql.DelayedMySQLSubscriberConfig{
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

	select {
	case <-messages:
		t.Errorf("message should not be received")
	case <-time.After(time.Millisecond * 200):
	}

	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		select {
		case received := <-messages:
			assert.Equal(t, msg.UUID, received.UUID)
			received.Ack()
		default:
			t.Errorf("message should be received")
		}
	}, time.Second, time.Millisecond*10)
}

func TestDelayedMySQL_NoDelay(t *testing.T) {
	t.Parallel()

	db := newMySQL(t)

	pub, err := sql.NewDelayedMySQLPublisher(db, sql.DelayedMySQLPublisherConfig{
		DelayPublisherConfig: delay.PublisherConfig{
			AllowNoDelay: true,
		},
		Logger: logger,
	})
	require.NoError(t, err)

	t.Run("skip_empty", func(t *testing.T) {
		t.Parallel()

		sub, err := sql.NewDelayedMySQLSubscriber(db, sql.DelayedMySQLSubscriberConfig{
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

		select {
		case <-messages:
			t.Errorf("message should not be received")
		case <-time.After(time.Second * 2):
		}
	})

	t.Run("allow_empty", func(t *testing.T) {
		t.Parallel()

		sub, err := sql.NewDelayedMySQLSubscriber(db, sql.DelayedMySQLSubscriberConfig{
			DeleteOnAck:  true,
			AllowNoDelay: true,
			Logger:       logger,
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
			case received := <-messages:
				assert.Equal(t, msg.UUID, received.UUID)
				received.Ack()
			default:
				t.Errorf("message should be received")
			}
		}, time.Second*2, time.Millisecond*10)
	})
}

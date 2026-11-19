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
		// Poll often, so that a message delivered before it is due would show up within the checks below.
		OverrideSubscriberConfig: func(config *sql.SubscriberConfig) error {
			config.PollInterval = 10 * time.Millisecond
			return nil
		},
		DeleteOnAck: true,
		Logger:      logger,
	})
	require.NoError(t, err)

	topic := watermill.NewUUID()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Not due for an hour and published first, so a subscriber that ignored the delay would deliver it right away,
	// and before msg.
	notDueMsg := message.NewMessage(watermill.NewUUID(), []byte("{}"))
	notDueMsg.SetContext(delay.WithContext(notDueMsg.Context(), delay.For(time.Hour)))

	err = pub.Publish(topic, notDueMsg)
	require.NoError(t, err)

	select {
	case received := <-messages:
		t.Errorf("message should not be received")
		received.Ack()
	case <-time.After(time.Millisecond * 200):
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("{}"))

	err = pub.Publish(topic, msg)
	require.NoError(t, err)

	select {
	case received := <-messages:
		assert.Equal(t, msg.UUID, received.UUID, "only the message that is due should be received")
		received.Ack()
	case <-time.After(15 * time.Second):
		t.Errorf("message should be received")
	}
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		messages, err := sub.Subscribe(ctx, topic)
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		messages, err := sub.Subscribe(ctx, topic)
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
		}, time.Second*15, time.Millisecond*10)
	})
}

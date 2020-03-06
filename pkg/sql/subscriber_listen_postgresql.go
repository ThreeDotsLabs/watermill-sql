package sql

import (
	"context"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type PostgresListenSubscriberConfig struct {
	// ConnectionString according to https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
	ConnectionString string

	MinReconnectInterval time.Duration
	MaxReconnectInterval time.Duration

	Unmarshaler PostgresListenUnmarshaler
}

func (c *PostgresListenSubscriberConfig) setDefaults() {
	if c.MinReconnectInterval == 0 {
		c.MinReconnectInterval = 200 * time.Millisecond
	}
	if c.MaxReconnectInterval == 0 {
		c.MaxReconnectInterval = 10 * time.Second
	}
}

func (c PostgresListenSubscriberConfig) validate() error {
	if c.ConnectionString == "" {
		return errors.New("connection string is empty")
	}
	if c.Unmarshaler == nil {
		return errors.New("unmarshaler is nil")
	}
	return nil
}

// PostgresListenSubscriber uses the Postgres LISTEN query to listen for NOTIFY events.
type PostgresListenSubscriber struct {
	consumerIdBytes  []byte
	consumerIdString string

	config PostgresListenSubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewPostgresListenSubscriber(config PostgresListenSubscriberConfig, logger watermill.LoggerAdapter) (*PostgresListenSubscriber, error) {
	config.setDefaults()
	err := config.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	idBytes, idStr, err := newSubscriberID()
	if err != nil {
		return &PostgresListenSubscriber{}, errors.Wrap(err, "cannot generate subscriber id")
	}
	logger = logger.With(watermill.LogFields{"subscriber_id": idStr})

	sub := &PostgresListenSubscriber{
		consumerIdBytes:  idBytes,
		consumerIdString: idStr,

		config: config,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}

	return sub, nil
}

func (s *PostgresListenSubscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err = validateTopicName(topic); err != nil {
		return nil, err
	}

	logger := s.logger.With(watermill.LogFields{
		"topic": topic,
	})

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	listener := pq.NewListener(
		s.config.ConnectionString,
		s.config.MinReconnectInterval,
		s.config.MaxReconnectInterval,
		nil,
	)

	logger.Debug("Listening", nil)
	err = listener.Listen(topic)
	if err != nil {
		return nil, errors.Wrap(err, "Error establishing listener")
	}

	go func() {
		for notification := range listener.NotificationChannel() {
			msg, err := s.config.Unmarshaler.Unmarshal(notification)
			if err != nil {
				logger.Error("Error unmarshaling notification to Watermill message", err, nil)
				continue
			}
			out <- msg
		}
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *PostgresListenSubscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

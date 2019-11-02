package sql

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type BinlogSubscriberConfig struct {
	SubscriberConfig
}

func (c *BinlogSubscriberConfig) setDefaults() {
	c.SubscriberConfig.setDefaults()
}

func (c BinlogSubscriberConfig) validate() error {
	if err := c.SubscriberConfig.validate(); err != nil {
		return err
	}
	return nil
}

type BinlogSubscriber struct {
	consumerIdBytes  []byte
	consumerIdString string

	db     *sql.DB
	config BinlogSubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewBinlogSubscriber(db *sql.DB, config BinlogSubscriberConfig, logger watermill.LoggerAdapter) (*BinlogSubscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
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
		return &BinlogSubscriber{}, errors.Wrap(err, "cannot generate subscriber id")
	}
	logger = logger.With(watermill.LogFields{"subscriber_id": idStr})

	sub := &BinlogSubscriber{
		consumerIdBytes:  idBytes,
		consumerIdString: idStr,

		db:     db,
		config: config,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}

	return sub, nil
}

func (s *BinlogSubscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err = validateTopicName(topic); err != nil {
		return nil, err
	}

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *BinlogSubscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
	})

	for {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			return

		default:
			// go on querying
		}

		// consume from canal
		messageUUID := ""
		err := errors.New("Fetch message from canal here")
		if err != nil && isDeadlock(err) {
			logger.Debug("Deadlock during querying message, trying again", watermill.LogFields{
				"err":          err.Error(),
				"message_uuid": messageUUID,
			})
		} else if err != nil {
			logger.Error("Error fetching message", err, nil)
		}
	}
}

func (s *BinlogSubscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

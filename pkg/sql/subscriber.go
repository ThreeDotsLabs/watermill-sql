package sql

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

var (
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

// SubscriberConfig holds the configuration common for all SQL subscribers.
type SubscriberConfig struct {
	ConsumerGroup string

	// ResendInterval is the time to wait before resending a nacked message.
	// Must be non-negative. Defaults to 1s.
	ResendInterval time.Duration
}

func (c *SubscriberConfig) setDefaults() {
	if c.ResendInterval == 0 {
		c.ResendInterval = time.Second
	}
}

func (c SubscriberConfig) validate() error {
	if c.ResendInterval <= 0 {
		return errors.New("resend interval must be a positive duration")
	}

	return nil
}

func newSubscriberID() ([]byte, string, error) {
	id := watermill.NewULID()
	idBytes, err := ulid.MustParseStrict(id).MarshalBinary()
	if err != nil {
		return nil, "", errors.Wrap(err, "cannot marshal subscriber id")
	}

	return idBytes, id, nil
}

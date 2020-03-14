package sql

import (
	"context"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// waitForNextQueryFn returns true when the subscriber is ready for a new query or bool when querying should be stopped.
type waitForNextQueryFn func(ctx context.Context, topic string) bool

func (s *Subscriber) waitForTime(ctx context.Context, topic string) bool {
	select {
	case <-time.After(s.config.PollInterval):
		return true
	case <-ctx.Done():
		return false
	case <-s.closing:
		return false
	}
}

func (s *Subscriber) waitForListen(topic string) (waitForNextQueryFn, error) {
	listener := pq.NewListener(topic, time.Millisecond, time.Second, nil)
	err := listener.Listen(topic)
	if err != nil {
		return nil, errors.Wrap(err, "could not setup LISTEN listener")
	}

	return func(ctx context.Context, topic string) bool {
		for {
			select {
			case notification := <-listener.NotificationChannel():
				if notification.Channel == topic {
					return true
				}
				continue
			case <-ctx.Done():
				return false
			case <-s.closing:
				return false
			}
		}
	}, nil
}

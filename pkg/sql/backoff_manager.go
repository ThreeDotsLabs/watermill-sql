package sql

import (
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

// BackoffManager handles errors or empty result sets and computes the backoff time.
// You could for example create a stateful version that computes a backoff depending on the error frequency or make errors more or less persistent.
type BackoffManager interface {
	// HandleError handles the error possibly logging it or returning a backoff time depending on the error or the absence of the message.
	HandleError(logger watermill.LoggerAdapter, noMsg bool, err error) time.Duration
}

func NewDefaultBackoffManager(pollInterval, retryInterval time.Duration) BackoffManager {
	if pollInterval == 0 {
		pollInterval = time.Second
	}
	if retryInterval == 0 {
		retryInterval = time.Second
	}
	return &defaultBackoffManager{
		retryInterval: retryInterval,
		pollInterval:  pollInterval,
		deadlockIndicators: []string{
			// MySQL deadlock indicator
			"deadlock",

			// PostgreSQL deadlock indicator
			"concurrent update",
		},
	}
}

type defaultBackoffManager struct {
	pollInterval       time.Duration
	retryInterval      time.Duration
	deadlockIndicators []string
}

func (d defaultBackoffManager) HandleError(logger watermill.LoggerAdapter, noMsg bool, err error) time.Duration {
	if err != nil {
		var deadlock bool
		for _, indicator := range d.deadlockIndicators {
			if strings.Contains(strings.ToLower(err.Error()), indicator) {
				deadlock = true
				break
			}
		}
		if deadlock {
			logger.Debug("Deadlock during querying message, trying again", watermill.LogFields{
				"err": err.Error(),
			})
			return 0
		} else {
			logger.Error("Error querying for message", err, watermill.LogFields{
				"wait_time": d.retryInterval,
			})
			return d.retryInterval
		}
	}
	if noMsg {
		return d.pollInterval
	}
	return 0
}

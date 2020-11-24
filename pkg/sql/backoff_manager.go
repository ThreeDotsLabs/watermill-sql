package sql

import (
	"github.com/ThreeDotsLabs/watermill"
	"strings"
	"time"
)

// BackoffManager handles errors or empty result sets and compute a backoff and a resulting error.
// You could for example create a stateful version that compute backoffs depending on the error frequency or make errors more or less persistent
type BackoffManager interface {
	// HandleError handle the error possibly logging it or returning a backoff time depending on the error or the absence of result.
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
	}
}

type defaultBackoffManager struct {
	pollInterval  time.Duration
	retryInterval time.Duration
}

func (d defaultBackoffManager) HandleError(logger watermill.LoggerAdapter, noMsg bool, err error) time.Duration {
	if err != nil {
		// ugly, but should be universal for multiple sql implementations
		if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
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

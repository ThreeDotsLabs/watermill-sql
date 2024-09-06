package sql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestValidateTopicName(t *testing.T) {
	schemaAdapter := sql.DefaultMySQLSchema{}
	offsetsAdapter := sql.DefaultMySQLOffsetsAdapter{}

	publisher, subscriber := newPubSub(t, newMySQL(t), "", schemaAdapter, offsetsAdapter)
	cleverlyNamedTopic := "some_topic; DROP DATABASE `watermill`"

	err := publisher.Publish(cleverlyNamedTopic, message.NewMessage("uuid", nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrInvalidTopicName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = subscriber.Subscribe(ctx, cleverlyNamedTopic)
	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrInvalidTopicName)
}

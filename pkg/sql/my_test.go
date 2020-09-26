package sql_test

import (
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySQLPublishJson(t *testing.T) {
	db := newMySQL(t)
	pub, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultMySQLSchema{},
		AutoInitializeSchema: true,
	}, watermill.NewStdLogger(true, true))

	require.NoError(t, err)
	err = pub.Publish("my_test", message.NewMessage(watermill.NewUUID(), []byte(`{"userID":"ef546861-b006-4d0c-bbd9-0010a9521e74","email":"john@doe.com"}`)))
	require.NoError(t, err)
}

type UserRegisteredEvent struct {
	UserID string `json:"userID"`
	Email  string `json:"email"`
}

func TestPostgresPublishJson(t *testing.T) {
	db := newPostgreSQL(t)
	pub, err := sql.NewPublisher(db, sql.PublisherConfig{
		SchemaAdapter:        sql.DefaultPostgreSQLSchema{},
		AutoInitializeSchema: true,
	}, watermill.NewStdLogger(true, true))

	require.NoError(t, err)

	payload, err := json.Marshal(&UserRegisteredEvent{
		UserID: "ef546861-b006-4d0c-bbd9-0010a9521e74",
		Email:  "john@doe.com",
	})
	require.NoError(t, err)

	err = pub.Publish("my_test", message.NewMessage(watermill.NewUUID(), payload))
	require.NoError(t, err)
}
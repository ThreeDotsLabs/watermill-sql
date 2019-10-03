package sql

import (
	"database/sql"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// SchemaAdapter produces the SQL queries and arguments appropriately for a specific schema and dialect
// It also transforms sql.Rows into Watermill messages.
type SchemaAdapter interface {
	// InsertQuery returns the SQL query and arguments that will insert the Watermill message into the SQL storage.
	InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error)

	// SelectQuery returns the the SQL query and arguments
	// that returns the next unread message for a given consumer group.
	SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) (string, []interface{})

	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	// It also returns the offset of the last read message, for the purpose of acking.
	UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error)

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []string
}

// Deprecated: Use DefaultMySQLSchema instead.
type DefaultSchema = DefaultMySQLSchema

type defaultSchemaRow struct {
	Offset   int64
	UUID     []byte
	Payload  []byte
	Metadata []byte
}

func defaultInsertArgs(msgs message.Messages) ([]interface{}, error) {
	var args []interface{}
	for _, msg := range msgs {
		metadata, err := json.Marshal(msg.Metadata)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal metadata into JSON for message %s", msg.UUID)
		}

		args = append(args, msg.UUID, msg.Payload, metadata)
	}

	return args, nil
}

func unmarshalDefaultMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	r := defaultSchemaRow{}
	err = row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return 0, nil, errors.Wrap(err, "could not scan message row")
	}

	msg = message.NewMessage(string(r.UUID), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return 0, nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return int(r.Offset), msg, nil
}

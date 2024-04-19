package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	stdSQL "database/sql"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultSQLite3Schema is a default implementation of SchemaAdapter based on Comfylite3. You could eventually use any other sqlite3 drivers, but you might have trouble depending on which one you will use. One common error you will encounter will be `database locked` when the driver is not handling the concurrent access properly.
//
// `comfylite3` is wrapper of [the famous go-sqlite3](https://github.com/mattn/go-sqlite3) driver which compensate the lack of goroutine management by giving the illusion of it. Most other libraries that re-implement the entire sqlite3 driver won't support the latest features, like the recent JSON datatype, sometimes you just want it all!
//
// Since you will need to provide a *sql.DB instance to the SQL adapter, you will need to use the `comfylite3` driver instead of the `go-sqlite3` driver.
//
// ```go
// go get -u github.com/davidroman0O/comfylite3
// ```
//
// Once you have the `comfylite3` driver, you can use the `comfylite3` driver in the following way:
//
// ```go
// comfydb, _ := comfylite3.Comfy(comfylite3.WithMemory()) // Options: WithMemory() or Withfile("path/to/file.db") or even writting your own connection WithConnection("file::memory:?_mutex=full&cache=shared&_timeout=5000")
// ```
//
// Then you can use the `comfydb` instance as the first parameter of `sql.NewSubscriber` or `sql.NewPublisher`.
//
// `comfydb` is exposing the interface of *sql.DB which allow you to use it as a regular driver, now you can have it all!
type DefaultSQLite3Schema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string

	// SubscribeBatchSize is the number of messages to be queried at once.
	//
	// Higher value, increases a chance of message re-delivery in case of crash or networking issues.
	// 1 is the safest value, but it may have a negative impact on performance when consuming a lot of messages.
	//
	// Default value is 100.
	SubscribeBatchSize int
}

func (s DefaultSQLite3Schema) SchemaInitializingQueries(topic string) []Query {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` INTEGER PRIMARY KEY AUTOINCREMENT,",
		"`uuid` TEXT NOT NULL,",
		"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		"`payload` TEXT,",
		"`metadata` TEXT",
		");",
	}, "\n")

	return []Query{{Query: createMessagesTable}}
}

func (s DefaultSQLite3Schema) InsertQuery(topic string, msgs message.Messages) (Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (uuid, payload, metadata) VALUES %s`,
		s.MessagesTable(topic),
		strings.TrimRight(strings.Repeat(`(?,?,?),`, len(msgs)), ","),
	)

	args, err := defaultInsertArgs(msgs)
	if err != nil {
		return Query{}, err
	}

	return Query{insertQuery, args}, nil
}

func (s DefaultSQLite3Schema) batchSize() int {
	if s.SubscribeBatchSize == 0 {
		return 100
	}

	return s.SubscribeBatchSize
}

func (s DefaultSQLite3Schema) SelectQuery(topic string, consumerGroup string, offsetsAdapter OffsetsAdapter) Query {
	nextOffsetQuery := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)

	selectQuery := "SELECT `offset`, `uuid`, `payload`, `metadata` FROM " + s.MessagesTable(topic) +
		" WHERE `offset` > (" + nextOffsetQuery.Query + ") ORDER BY `offset` ASC" +
		` LIMIT ` + fmt.Sprintf("%d", s.batchSize())

	return Query{Query: selectQuery, Args: nextOffsetQuery.Args}
}

func (s DefaultSQLite3Schema) UnmarshalMessage(row Scanner) (Row, error) {
	r := Row{}
	err := row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return Row{}, errors.Join(err, errors.New("could not scan message row"))
	}

	msg := message.NewMessage(string(r.UUID), []byte(r.Payload))

	if r.Metadata != nil {
		err = json.Unmarshal([]byte(r.Metadata), &msg.Metadata)
		if err != nil {
			return Row{}, errors.Join(err, errors.New("could not unmarshal metadata as JSON"))
		}
	}

	r.Msg = msg

	return r, nil
}

func (s DefaultSQLite3Schema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return s.GenerateMessagesTableName(topic)
	}
	return fmt.Sprintf("watermill_%s", topic)
}

func (s DefaultSQLite3Schema) SubscribeIsolationLevel() stdSQL.IsolationLevel {
	// SQLite does not support isolation levels, so we return the default level.
	return stdSQL.LevelDefault
}

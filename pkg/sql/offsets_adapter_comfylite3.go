package sql

import "fmt"

// DefaultComfylite3OffsetsAdapter is a default implementation of OffsetsAdapter based on Comfylite3. You could eventually use any other sqlite3 drivers, but you might have trouble depending on which one you will use. One common error you will encounter will be `database locked` when the driver is not handling the concurrent access properly.
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
type DefaultComfylite3OffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultComfylite3OffsetsAdapter) SchemaInitializingQueries(topic string) []Query {
	return []Query{
		{
			Query: `
				CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
				consumer_group TEXT NOT NULL,
				offset_acked INTEGER,
				offset_consumed INTEGER NOT NULL,
				PRIMARY KEY(consumer_group)
			)`,
		},
	}
}

func (a DefaultComfylite3OffsetsAdapter) AckMessageQuery(topic string, row Row, consumerGroup string) Query {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, offset_acked, consumer_group)
		VALUES (?, ?, ?) ON CONFLICT(consumer_group) DO UPDATE SET offset_consumed=excluded.offset_consumed, offset_acked=excluded.offset_acked`

	return Query{ackQuery, []any{row.Offset, row.Offset, consumerGroup}}
}

func (a DefaultComfylite3OffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) Query {
	return Query{
		Query: `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group=?
				), 0)`,
		Args: []any{consumerGroup},
	}
}

func (a DefaultComfylite3OffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("watermill_offsets_%s", topic)
}

func (a DefaultComfylite3OffsetsAdapter) ConsumedMessageQuery(topic string, row Row, consumerGroup string, consumerULID []byte) Query {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES (?, ?) ON CONFLICT(consumer_group) DO UPDATE SET offset_consumed=excluded.offset_consumed`
	return Query{ackQuery, []interface{}{row.Offset, consumerGroup}}
}

func (a DefaultComfylite3OffsetsAdapter) BeforeSubscribingQueries(topic, consumerGroup string) []Query {
	return nil
}

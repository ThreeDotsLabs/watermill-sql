package sql

type OffsetsAdapter interface {
	// AckMessageQuery the SQL query and arguments that will mark a message as read for a given consumer group.
	AckMessageQuery(topic string, row Row, consumerGroup string) Query

	// ConsumedMessageQuery will return the SQL query and arguments which be executed after consuming message,
	// but before ack.
	//
	// ConsumedMessageQuery is optional, and will be not executed if query is empty.
	ConsumedMessageQuery(topic string, row Row, consumerGroup string, consumerULID []byte) Query

	// NextOffsetQuery returns the SQL query and arguments which should return offset of next message to consume.
	NextOffsetQuery(topic, consumerGroup string) Query

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []Query

	// BeforeSubscribingQueries returns queries which will be executed before subscribing to a topic.
	// All queries will be executed in a single transaction.
	BeforeSubscribingQueries(topic string, consumerGroup string) []Query
}

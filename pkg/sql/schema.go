package sql

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
)

func initializeSchema(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	db ContextExecutor,
	schemaAdapter SchemaAdapter,
	offsetsAdapter OffsetsAdapter,
) error {
	err := validateTopicName(topic)
	if err != nil {
		return err
	}

	initializingQueries := schemaAdapter.SchemaInitializingQueries(topic)
	if offsetsAdapter != nil {
		initializingQueries = append(initializingQueries, offsetsAdapter.SchemaInitializingQueries(topic)...)
	}

	logger.Info("Initializing subscriber schema", watermill.LogFields{
		"query": initializingQueries,
	})

	for _, q := range initializingQueries {
		_, err = db.ExecContext(ctx, q.Query, q.Args...)
		if err != nil {
			return fmt.Errorf("could not initialize schema: %w", err)
		}
	}

	return nil
}

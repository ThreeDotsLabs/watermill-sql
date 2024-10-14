package sql

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

func initializeSchema(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	db Beginner,
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "could not start transaction")
	}

	for _, q := range initializingQueries {
		_, err := tx.ExecContext(ctx, q.Query, q.Args...)
		if err != nil {
			return errors.Wrap(err, "could not initialize schema")
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "could not commit transaction")
	}

	return nil
}

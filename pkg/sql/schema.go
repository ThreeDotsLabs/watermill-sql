package sql

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

type RequiresTransaction interface {
	// RequiresTransaction returns true if the schema adapter requires a transaction to be started before executing queries.
	RequiresTransaction() bool
}

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

	if rt, ok := schemaAdapter.(RequiresTransaction); ok && rt.RequiresTransaction() {
		err = initialiseInTx(ctx, db, initializingQueries)
		if err != nil {
			return errors.Wrap(err, "could not initialize schema in transaction")
		}
	}

	for _, q := range initializingQueries {
		_, err := db.ExecContext(ctx, q.Query, q.Args...)
		if err != nil {
			return errors.Wrap(err, "could not initialize schema")
		}
	}

	return nil
}

func initialiseInTx(ctx context.Context, db ContextExecutor, initializingQueries []Query) error {
	beginner, ok := db.(Beginner)
	if !ok {
		return errors.New("db is not a Beginner")
	}

	err := runInTx(ctx, beginner, func(ctx context.Context, tx Tx) error {
		for _, q := range initializingQueries {
			_, err := tx.ExecContext(ctx, q.Query, q.Args...)
			if err != nil {
				return errors.Wrap(err, "could not initialize schema")
			}
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "run in tx")
	}

	return nil
}

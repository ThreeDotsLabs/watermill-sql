package sql

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
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

	initializingQueries, err := schemaAdapter.SchemaInitializingQueries(SchemaInitializingQueriesParams{
		Topic: topic,
	})
	if err != nil {
		return fmt.Errorf("could not generate schema initializing queries: %w", err)
	}

	if offsetsAdapter != nil {
		queries, err := offsetsAdapter.SchemaInitializingQueries(OffsetsSchemaInitializingQueriesParams{
			Topic: topic,
		})
		if err != nil {
			return fmt.Errorf("could not generate offset adapter's schema initializing queries: %w", err)
		}
		initializingQueries = append(initializingQueries, queries...)
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

	return initialise(ctx, db, initializingQueries)
}

func initialise(ctx context.Context, db ContextExecutor, initializingQueries []Query) error {
	for _, q := range initializingQueries {
		_, err = db.ExecContext(ctx, q.Query, q.Args...)
		if err != nil {
			return fmt.Errorf("could not initialize schema: %w", err)
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
		return initialise(ctx, tx, initializingQueries)
	})
	if err != nil {
		return errors.Wrap(err, "run in tx")
	}

	return nil
}

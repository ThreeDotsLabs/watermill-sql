package sql

import (
	"context"
	"errors"
	"fmt"
)

func runInTx(
	ctx context.Context,
	db Beginner,
	fn func(ctx context.Context, tx Tx) error,
) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
			return
		}

		err = tx.Commit()
	}()

	return fn(ctx, tx)
}

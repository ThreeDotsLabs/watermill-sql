package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// todo: use in other places?
func runInTx(
	ctx context.Context,
	db Beginner,
	fn func(ctx context.Context, tx *sql.Tx) error,
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

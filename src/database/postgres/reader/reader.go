package database

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	database "github.com/linusgith/migration-worker/src/database/postgres/sqlc"
	"go.uber.org/zap"
)

type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) GetMigrationJob(ctx context.Context, uuid string) (database.DbMigration, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return database.DbMigration{}, fmt.Errorf("beeginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	workers, err := q.GetMigrationJob(ctx, pgtype.UUID{
		Bytes: [16]byte([]byte(uuid)),
		Valid: true,
	})
	if err != nil {
		return database.DbMigration{}, fmt.Errorf("getting all worker states failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return database.DbMigration{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got workers state")
	return workers, nil

}

package postgres

import (
	"context"
	"fmt"
	guuid "github.com/google/uuid"
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

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return database.DbMigration{}, fmt.Errorf("could not parse uuid (%s): %w", uuid, err)
	}

	q := database.New(tx)
	workers, err := q.GetMigrationJob(ctx, pgtype.UUID{
		Bytes: parsed,
		Valid: true,
	})
	if err != nil {
		return database.DbMigration{}, fmt.Errorf("getting migration job: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return database.DbMigration{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got migration job")
	return workers, nil

}

func (r *Reader) GetAllMappings(ctx context.Context) ([]database.DbMapping, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	mappings, err := q.GetDBMappings(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all db mappings failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got db mappings", zap.Int("count", len(mappings)))
	return mappings, nil

}

func (r *Reader) DoIExist(ctx context.Context, uuid string) error {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return fmt.Errorf("could not parse uuid: %s", uuid)
	}

	q := database.New(tx)
	_, err = q.CheckInTable(ctx, pgtype.UUID{
		Bytes: parsed,
		Valid: false,
	})
	if err != nil {
		return fmt.Errorf("this worker does not exist in the migration worker table: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	return nil

}

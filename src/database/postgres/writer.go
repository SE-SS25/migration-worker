package postgres

import (
	"context"
	"fmt"
	guuid "github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	database "github.com/linusgith/migration-worker/src/database/postgres/sqlc"
	oe "github.com/linusgith/migration-worker/src/errors"
	"github.com/linusgith/migration-worker/src/utils"
	"go.uber.org/zap"
	"time"
)

type Writer struct {
	Logger *zap.Logger
	Pool   *pgxpool.Pool
}

func (w *Writer) Heartbeat(ctx context.Context, uuid string, interval time.Duration) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid (%s): %w", uuid, err), Reconcilable: false}
	}

	now := time.Now()
	args := database.HeartbeatParams{
		ID: pgtype.UUID{
			Bytes: parsed,
			Valid: true,
		},
		LastHeartbeat: pgtype.Timestamptz{
			Time:             now,
			InfinityModifier: 0,
			Valid:            true,
		},
		Uptime: pgtype.Interval{
			Microseconds: interval.Microseconds(),
			Days:         0,
			Months:       0,
			Valid:        true,
		},
	}
	execRes, execErr := q.Heartbeat(ctx, args)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	return oe.DbError{Err: nil}

}

func (w *Writer) UpdateJobStatus(ctx context.Context, uuid string, status utils.JobStatus) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid (%s): %w", uuid, err), Reconcilable: false}
	}

	q := database.New(tx)
	args := database.ChangeJobStatusParams{
		MWorkerID: pgtype.UUID{
			Bytes: parsed,
			Valid: true,
		},
		Status: database.MigrationStatus(status),
	}
	execRes, execErr := q.ChangeJobStatus(ctx, args)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully changed status", zap.String("worker_uuid", uuid), zap.String("newStatus", string(status)))
	return oe.DbError{Err: nil}

}

func (w *Writer) RemoveSelf(ctx context.Context, uuid string) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid (%s): %w", uuid, err), Reconcilable: false}
	}

	q := database.New(tx)
	execRes, execErr := q.RemoveSelf(ctx, pgtype.UUID{
		Bytes: parsed,
		Valid: true,
	})
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully removed self from table", zap.String("workerID", uuid))
	return oe.DbError{Err: nil}
}

package utils

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"github.com/linusgith/migration-worker/src/errors"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

type JobStatus string

func (j JobStatus) isValid() bool {
	switch j {
	case Running, Waiting, Failed, Done:
		return true
	default:
		return false
	}
}

const (
	Running JobStatus = "running"
	Waiting JobStatus = "wating"
	Failed  JobStatus = "failed"
	Done    JobStatus = "done"
)

func SetupPGConn(ctx context.Context, logger *zap.Logger) (*pgxpool.Pool, error) {

	pgConn := goutils.Log().ParseEnvStringPanic("PG_CONN", logger)
	logger.Debug("Connecting to database", zap.String("conn_string", pgConn))

	pool, err := pgxpool.New(ctx, pgConn)
	if err != nil {
		logger.Error("Unable to connect to database", zap.Error(err))
		return nil, err
	}

	if err = pool.Ping(ctx); err != nil {
		logger.Error("Unable to ping database", zap.Error(err))
		return nil, err
	}

	logger.Info("Connected to PG database", zap.String("conn", pgConn))

	return pool, nil
}

func SetupMongoConn(logger *zap.Logger, uri string) (*mongo.Client, error) {

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		logger.Error("could not connect to database", zap.String("mongoUri", uri))
		return &mongo.Client{}, err
	}

	return client, nil

}

func Must(execRes pgconn.CommandTag, execErr error) errors.DbError {
	if execErr != nil {
		return errors.DbError{
			Err:          fmt.Errorf("execution error occurred: %w", execErr),
			Reconcilable: true,
		}
	}
	if execRes.RowsAffected() == 0 {
		return errors.DbError{
			Err:          fmt.Errorf("no execution error but no rows affected: %s", execRes.String()),
			Reconcilable: false,
		}
	}

	return errors.DbError{Err: nil}
}

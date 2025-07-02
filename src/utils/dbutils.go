package utils

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"os"
)

func SetupDBConn(logger *zap.Logger, ctx context.Context) (*pgxpool.Pool, error) {

	pgConn := os.Getenv("PG_CONN")
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

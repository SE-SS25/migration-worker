package database

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Writer struct {
	Logger *zap.Logger
	Pool   *pgxpool.Pool
}

package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	database "github.com/linusgith/migration-worker/src/database/postgres/sqlc"
	ownErrors "github.com/linusgith/migration-worker/src/errors"
	"go.uber.org/zap"
	"math"
	"strconv"
	"time"
)

type ReaderPerfectionist struct {
	reader         *Reader
	maxBackoff     time.Duration
	initialBackoff time.Duration
	backoffType    string
}

func NewReaderPerfectionist(reader *Reader) *ReaderPerfectionist {

	//TODO ugly with the loggers

	//15 ms in exp backoff gives us {15, 225, 3375} ms as backoff intervals
	//we shouldn't allow a long backoff for the controller since shit can hit the fan fast
	initBackoff := goutils.Log().ParseEnvDurationDefault("INIT_RETRY_BACKOFF", 15*time.Millisecond, reader.Logger)

	maxBackoff := goutils.Log().ParseEnvDurationDefault("MAX_BACKOFF", 5*time.Minute, reader.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := goutils.Log().ParseEnvStringDefault("BACKOFF_TYPE", defaultBackoffStrategy, reader.Logger)

	var backoffType string

	switch backoffTypeInput {
	case "exp":
		backoffType = "exponential"
	case "lin":
		backoffType = "linear"
	default:
		reader.Logger.Warn("invalid backoff strategy provided, setting default", zap.String("provided", backoffTypeInput))
		backoffType = defaultBackoffStrategy
	}

	return &ReaderPerfectionist{
		reader:         reader,
		maxBackoff:     maxBackoff,
		initialBackoff: initBackoff,
		backoffType:    backoffType,
	}
}

func (r *ReaderPerfectionist) GetMigrationJob(ctx context.Context, mWorkerId string) (database.DbMigration, error) {
	var err error

	backoff := r.initialBackoff
	count := 1.0

	for backoff <= r.maxBackoff {
		migrationJob, err := r.reader.GetMigrationJob(ctx, mWorkerId)
		if err == nil {
			return migrationJob, nil
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return database.DbMigration{}, err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > r.maxBackoff.Milliseconds() {
			backoff = r.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	r.reader.Logger.Warn("getMigrationJob", zap.Error(ownErrors.UnreachableCode))
	return database.DbMigration{}, err // unreachable code
}

func (r *ReaderPerfectionist) GetAllMappings(ctx context.Context) ([]database.DbMapping, error) {

	var err error

	backoff := r.initialBackoff
	count := 1.0

	for backoff <= r.maxBackoff {
		migrationJob, err := r.reader.GetAllMappings(ctx)
		if err == nil {
			return migrationJob, nil
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > r.maxBackoff.Milliseconds() {
			backoff = r.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		r.reader.Logger.Info("new backoff", zap.Duration("backoff", backoff))
		count++
		time.Sleep(backoff)
	}

	r.reader.Logger.Warn("getDbMappings", zap.Error(ownErrors.UnreachableCode))
	return nil, err // unreachable code

}

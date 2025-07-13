package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	ownErrors "github.com/linusgith/migration-worker/src/errors"
	"github.com/linusgith/migration-worker/src/utils"
	"go.uber.org/zap"
	"math"
	"strconv"
	"time"
)

type WriterPerfectionist struct {
	writer         *Writer
	maxBackoff     time.Duration
	initialBackoff time.Duration
	backoffType    string
}

func NewWriterPerfectionist(writer *Writer) *WriterPerfectionist {

	//TODO ugly with the loggers

	//15 ms in exp backoff gives us [15,225, 3375] ms as backoff intervals
	//we shouldn't allow a long backoff for the controller since shit can hit the fan fast
	initBackoff := goutils.Log().ParseEnvDurationDefault("INIT_RETRY_BACKOFF", 15*time.Millisecond, writer.Logger)

	maxBackoff := goutils.Log().ParseEnvDurationDefault("MAX_BACKOFF", 5*time.Minute, writer.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := goutils.Log().ParseEnvStringDefault("BACKOFF_TYPE", defaultBackoffStrategy, writer.Logger)

	var backoffType string

	switch backoffTypeInput {
	case "exp":
		backoffType = "exponential"
	case "lin":
		backoffType = "linear"
	default:
		writer.Logger.Warn("invalid backoff strategy provided, setting default", zap.String("provided", backoffTypeInput))
		backoffType = defaultBackoffStrategy
	}

	return &WriterPerfectionist{
		writer:         writer,
		maxBackoff:     maxBackoff,
		initialBackoff: initBackoff,
		backoffType:    backoffType,
	}
}

func (w *WriterPerfectionist) Heartbeat(ctx context.Context, uuid string, interval time.Duration) error {

	var err error

	backoff := w.initialBackoff
	count := 1.0

	for backoff <= w.maxBackoff {
		oeErr := w.writer.Heartbeat(ctx, uuid, interval)
		if oeErr.Err == nil {
			return nil
		}

		if !oeErr.Reconcilable {
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			continue
		}

		backoff, _ = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	w.writer.Logger.Warn("heartbeat", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code

}

func (w *WriterPerfectionist) UpdateJobStatus(ctx context.Context, uuid string, status utils.JobStatus) error {

	var err error

	backoff := w.initialBackoff
	count := 1.0

	for backoff <= w.maxBackoff {
		oeErr := w.writer.UpdateJobStatus(ctx, uuid, status)
		if oeErr.Err == nil {
			return nil
		}

		if !oeErr.Reconcilable {
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	w.writer.Logger.Warn("updateJobStatus", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code

}

func (w *WriterPerfectionist) RemoveJob(ctx context.Context, workerId string) error {

	var err error

	backoff := w.initialBackoff
	count := 1.0

	w.writer.Logger.Debug("removeJob", zap.String("workerId", workerId), zap.Duration("initialBackoff", w.initialBackoff), zap.Duration("maxBackoff", w.maxBackoff))

	for backoff <= w.maxBackoff {
		oeErr := w.writer.DeleteWorkerJobJoin(ctx, workerId)
		if oeErr.Err == nil {
			w.writer.Logger.Debug("removeJob successful", zap.String("workerId", workerId))
			return nil
		}

		if !oeErr.Reconcilable {
			w.writer.Logger.Error("removeJob failed", zap.String("workerId", workerId), zap.Error(oeErr.Err))
			return err
		}

		oeErr = w.writer.RemoveJob(ctx, workerId)
		if oeErr.Err == nil {
			w.writer.Logger.Debug("removeJob successful", zap.String("workerId", workerId))
			return nil
		}

		if !oeErr.Reconcilable {
			w.writer.Logger.Error("removeJob failed", zap.String("workerId", workerId), zap.Error(oeErr.Err))
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			w.writer.Logger.Debug("removeJob no rows found", zap.String("workerId", workerId))
			return err
		}

		w.writer.Logger.Debug("removeJob retrying", zap.String("workerId", workerId), zap.Error(oeErr.Err), zap.Duration("backoff", backoff))

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			time.Sleep(backoff)
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		time.Sleep(backoff)
		count++
	}

	w.writer.Logger.Warn("removeJob", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code

}

func (w *WriterPerfectionist) RemoveDbMapping(ctx context.Context, mappingId string) error {

	var err error

	backoff := w.initialBackoff
	count := 1.0

	for backoff <= w.maxBackoff {
		oeErr := w.writer.RemoveDbMapping(ctx, mappingId)
		if oeErr.Err == nil {
			return nil
		}

		if !oeErr.Reconcilable {
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	w.writer.Logger.Warn("removeDbMapping", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code

}

func (w *WriterPerfectionist) AddDbMapping(ctx context.Context, from, url string) error {
	var err error

	backoff := w.initialBackoff
	count := 1.0

	for backoff <= w.maxBackoff {
		oeErr := w.writer.AddDbMapping(ctx, from, url)
		if oeErr.Err == nil {
			return nil
		}

		if !oeErr.Reconcilable {
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		w.writer.Logger.Debug("addDbMapping retrying", zap.String("from", from), zap.String("url", url), zap.Error(oeErr.Err), zap.Duration("backoff", backoff))
		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	w.writer.Logger.Warn("addDbMapping", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code
}

func (w *WriterPerfectionist) RemoveSelf(ctx context.Context, uuid string) error {

	var err error

	backoff := w.initialBackoff
	count := 1.0

	for backoff <= w.maxBackoff {
		oeErr := w.writer.RemoveSelf(ctx, uuid)
		if oeErr.Err == nil {
			return nil
		}

		if !oeErr.Reconcilable {
			return err
		}

		if errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		backoffMs := backoff.Milliseconds()
		newBackoffMs := int64(math.Pow(float64(backoffMs), count))
		if newBackoffMs > w.maxBackoff.Milliseconds() {
			backoff = w.maxBackoff
			continue
		}

		backoff, err = time.ParseDuration(strconv.FormatInt(newBackoffMs, 10) + "ms")
		count++
	}

	w.writer.Logger.Warn("removeSelf", zap.Error(ownErrors.UnreachableCode))
	return err // unreachable code

}

package database

import (
	"github.com/linusgith/migration-worker/src/utils"
	"go.uber.org/zap"
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
	initBackoff := utils.ParseEnvDuration("INIT_RETRY_BACKOFF", 15*time.Millisecond, writer.Logger)

	maxBackoff := utils.ParseEnvDuration("MAX_BACKOFF", 5*time.Minute, reader.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := utils.ParseEnvStringWithDefault("BACKOFF_TYPE", defaultBackoffStrategy, writer.Logger)

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

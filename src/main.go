package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/goforj/godump"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"github.com/linusgith/migration-worker/src/database/postgres"
	"github.com/linusgith/migration-worker/src/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"time"
)

func createProductionLogger() *zap.Logger {
	stdout := zapcore.AddSync(os.Stdout)

	file := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "logs/app.log",
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     7, // days
	})

	level := zap.NewAtomicLevelAt(zap.InfoLevel)

	productionCfg := zap.NewProductionEncoderConfig()
	productionCfg.TimeKey = "timestamp"
	productionCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	developmentCfg := zap.NewDevelopmentEncoderConfig()
	developmentCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	consoleEncoder := zapcore.NewConsoleEncoder(developmentCfg)
	fileEncoder := zapcore.NewJSONEncoder(productionCfg)

	core := zapcore.NewTee(
		zapcore.NewSamplerWithOptions(zapcore.NewCore(consoleEncoder, stdout, level), time.Second, 10, 5),
		zapcore.NewCore(fileEncoder, file, level),
	)

	logger := zap.New(core)

	logger.Info("Production logger created")
	godump.Dump(core)

	return logger
}

func createDevelopmentLogger() *zap.Logger {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.TimeKey = " timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	config := zap.Config{
		Level:         zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:   true,
		Encoding:      "json",
		EncoderConfig: encoderCfg,
		OutputPaths: []string{
			"stderr",
		},
	}

	logger := zap.Must(config.Build())

	logger.Info("Development logger created")
	godump.Dump(logger.Core())

	return logger
}

func setupStructs(pool *pgxpool.Pool, logger *zap.Logger) MigrationWorker {

	reader := postgres.Reader{
		Pool:   pool,
		Logger: logger,
	}

	writer := postgres.Writer{
		Pool:   pool,
		Logger: logger,
	}

	readerPerf := postgres.NewReaderPerfectionist(&reader)

	writerPerf := postgres.NewWriterPerfectionist(&writer)

	migrationMonster := MigrationWorker{
		uuid:       goutils.Log().ParseEnvStringPanic("UUID", logger),
		logger:     logger,
		reader:     &reader,
		writer:     &writer,
		writerPerf: writerPerf,
		readerPerf: readerPerf,
	}

	return migrationMonster

}

func main() {

	//Setting a context without a timeout since this main function should (optimally) run forever
	ctx := context.Background()

	var logger *zap.Logger

	//Configure the logger depending on app environment
	env := goutils.NoLog().ParseEnvStringPanic("APP_ENV")

	switch env {
	case "prod":
		logger = createProductionLogger()
	case "dev":
		logger = createDevelopmentLogger()
	default:
		fmt.Printf("Logger creation failed, since an invalid app environment was specified: %s", env)
		return
	}
	logger.Info("Logger initialized", zap.String("environment", env))

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			println("Error closing the logger")
		}
	}(logger)

	pool, err := utils.SetupPGConn(ctx, logger)
	if err != nil {
		logger.Fatal("establishing connection to database failed, controller is fucking useless, stopping...", zap.Error(err))
		return
	}

	mm := setupStructs(pool, logger)

	go checkDeleted(ctx, mm)
	go mm.Heartbeat(ctx)

	err = runMigrationLoop(ctx, mm)
	if err != nil {
		logger.Fatal("the migration loop failed and emitted an error", zap.Error(err))
	}

	//Remove self from the migration workers table and then shut down

	err = mm.writerPerf.RemoveSelf(ctx, mm.uuid)
	if err != nil {
		logger.Fatal("could not remove self from the migration workers table", zap.Error(err))
		return
	}

	os.Exit(0)
}

func runMigrationLoop(ctx context.Context, mm MigrationWorker) error {

	retries := goutils.Log().ParseEnvIntDefault("RETRIES", 5, mm.logger)
	first := true

	for i := 0; i < retries; i++ {
		if i != 0 {
			time.Sleep(5 * time.Second)
		}
		//since there is no limit to the backoff, if this fails over and over, we won't get anywhere
		//TODO either change functionality here or handle this in the controller
		job, getJobErr := mm.readerPerf.GetMigrationJob(ctx, mm.uuid)
		if errors.Is(getJobErr, pgx.ErrNoRows) {
			if first {
				mm.logger.Warn("migration worker was started but there is no available job", zap.String("m_worker_id", mm.uuid))
				first = false
			} else {
				mm.logger.Info("there is no available job for this migration worker", zap.String("m_worker_id", mm.uuid))
			}

			//go to next iteration and check if there is a job now
			continue
		}
		if getJobErr != nil {
			mm.logger.Error("could not get migration job, but error was not 'no rows'", zap.Error(getJobErr))
			return getJobErr
		}

		err := mm.writerPerf.UpdateJobStatus(ctx, mm.uuid, utils.Running)
		if err != nil {
			mm.logger.Error("migration worker took job but could not update the db state", zap.String("workerId", mm.uuid), zap.String("state", string(utils.Running)))
			return err
		}

		originCli, destCli, prepErr := mm.PrepareMigration(ctx, job)
		if prepErr != nil {
			mm.logger.Error("preparing migration failed", zap.String("migrationId", job.ID.String()))
			return prepErr
		}

		//execute migration

		newStatus := utils.Done

		execErr := mm.RunMigration(ctx, originCli, destCli, job)
		if execErr != nil {
			mm.logger.Error("error occurred running the migration", zap.String("jobId", job.ID.String()))
			newStatus = utils.Failed
		}

		//update the status
		err = mm.writerPerf.UpdateJobStatus(ctx, mm.uuid, newStatus)
		if err != nil {
			mm.logger.Error("could not update the job status to new state", zap.String("jobId", job.ID.String()), zap.String("newStatus", string(newStatus)))
			return err
		}

		//reset the counter if migration was found and successful
		i = 0

	}

	return nil
}

func checkDeleted(ctx context.Context, mm MigrationWorker) {

	for {
		err := mm.reader.DoIExist(ctx, mm.uuid)
		if err != nil {
			mm.logger.Fatal("the worker does not exist in the migration workers table anymore, exiting...")
		}

		time.Sleep(2 * time.Second)
	}
}

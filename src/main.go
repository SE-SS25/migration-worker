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

	err = runMigration(ctx, mm)
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

func runMigration(ctx context.Context, mm MigrationWorker) error {

	job, getJobErr := mm.readerPerf.GetMigrationJob(ctx, mm.uuid)
	if errors.Is(getJobErr, pgx.ErrNoRows) {

		mm.logger.Info("there is no available job for this migration worker", zap.String("m_worker_id", mm.uuid))

	}
	if getJobErr != nil {
		mm.logger.Error("could not get migration job, but error was not 'no rows'", zap.Error(getJobErr))
		return getJobErr
	}

	//if job is 'failed', 'done' (but not removed), 'running' (but not set running by this instance) we enter failure mode
	if job.Status != "waiting" {
		mm.failureMode = true
	}

	//prepare
	err := mm.writerPerf.UpdateJobStatus(ctx, mm.uuid, utils.Running)
	if err != nil {
		mm.logger.Error("migration worker took job but could not update the db state", zap.String("workerId", mm.uuid), zap.String("state", string(utils.Running)))
		return err
	}

	originCli, destCli, prepErr := mm.PrepareMigration(ctx, job)
	if prepErr != nil {
		mm.logger.Error("preparing migration failed", zap.String("migrationId", job.ID.String()), zap.Error(prepErr))
		return prepErr
	}

	//execute
	execErr := mm.Migrate(ctx, originCli, destCli, job)
	if execErr != nil {
		mm.logger.Error("error occurred running the migration", zap.String("jobId", job.ID.String()))
	}

	mm.logger.Info("migration worker finished running the migration", zap.String("jobId", job.ID.String()), zap.String("workerId", mm.uuid))

	//cleanup
	err = mm.writerPerf.RemoveJob(ctx, mm.uuid)
	if err != nil {
		mm.logger.Error("could not update the job status to new state", zap.String("jobId", job.ID.String()))
		return err
	}

	mm.logger.Info("migration worker successfully removed the job from the migration workers table", zap.String("jobId", job.ID.String()), zap.String("workerId", mm.uuid))

	//TODO change db_mappings table accordingly

	mappings, err := mm.readerPerf.GetAllMappings(ctx)
	if err != nil {
		return fmt.Errorf("could not get all db mappings: %w", err)
	}

	//crete the new db mappings

	err = mm.writerPerf.AddDbMapping(ctx, job.From, job.Url)
	if err != nil {
		return err
	}

	url, err := getOriginUrl(mappings, job)
	if err != nil {
		return fmt.Errorf("could not get origin url from mappings: %w", err)
	}

	err = mm.writerPerf.AddDbMapping(ctx, job.To, url)
	if err != nil {
		return fmt.Errorf("could not add db mapping for destination: %w", err)
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

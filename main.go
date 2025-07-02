package main

import (
	"context"
	"fmt"
	"github.com/goforj/godump"
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

func main() {

	//Setting a context without a timeout since this main function should (optimally) run forever
	ctx := context.Background()

	var logger *zap.Logger

	//Configure the logger depending on app environment
	env := os.Getenv("APP_ENV")

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

	pool, err := utils.SetupDBConn(logger, ctx)
	if err != nil {
		logger.Fatal("establishing connection to database failed, controller is fucking useless, stopping...", zap.Error(err))
		return
		//TODO retries
	}
package log

import (
	"DeltaReceiver/pkg/env"
	"go.uber.org/zap"
)

func GetLogger(loggerName string) *zap.Logger {
	envType := env.GetEnvType()
	var logger *zap.Logger
	var err error
	if envType == env.DEV {
		cfg := zap.NewDevelopmentConfig()
		cfg.OutputPaths = []string{
			"/var/log/writer/writer.log",
		}
		logger, err = cfg.Build()
	} else if envType == env.PROD {
		cfg := zap.NewProductionConfig()
		cfg.OutputPaths = []string{
			"/var/log/writer/writer.log",
		}
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	return logger.Named(loggerName)
}

func GetTestLogger() *zap.Logger {
	return zap.NewNop()
}

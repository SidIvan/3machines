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
		logger, err = zap.NewDevelopment(
			zap.Development(),
		)
	} else if envType == env.PROD {
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

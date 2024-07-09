package log

import (
	"DeltaReceiver/pkg/env"
	"fmt"
	"go.uber.org/zap"
)

var serviceName string

func InitServiceName(name string) {
	serviceName = name
}

func GetCustomLogger(loggerName string, fileName string) *zap.Logger {
	envType := env.GetEnvType()
	var logger *zap.Logger
	var err error
	if envType == env.DEV {
		cfg := zap.NewDevelopmentConfig()
		cfg.OutputPaths = []string{
			fmt.Sprintf("/app/log/%s.log", fileName),
		}
		logger, err = cfg.Build()
	} else if envType == env.PROD {
		cfg := zap.NewProductionConfig()
		cfg.OutputPaths = []string{
			fmt.Sprintf("/app/log/%s.log", fileName),
		}
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	return logger.Named(loggerName)
}

func GetLogger(loggerName string) *zap.Logger {
	return GetCustomLogger(loggerName, serviceName)
}

func GetTestLogger() *zap.Logger {
	return zap.NewNop()
}

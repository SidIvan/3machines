package log

import (
	"DeltaReceiver/pkg/env"
	"fmt"
	"net/http"
	"time"

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

func GetSTDOutLogger(loggerName string) *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger.Named(loggerName)
}

func GetLogger(loggerName string) *zap.Logger {
	if serviceName == "" {
		return GetSTDOutLogger(loggerName)
	}
	return GetCustomLogger(loggerName, serviceName)
}

func GetTestLogger() *zap.Logger {
	return zap.NewNop()
}

func CreateMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(httpHandler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Info(fmt.Sprintf("Request: %s %s %s\n", r.Method, r.RequestURI, time.Now().Format(time.RFC822)))
			httpHandler.ServeHTTP(w, r)
			logger.Info(fmt.Sprintf("Response: %s %s %s\n", r.Method, r.RequestURI, time.Now().Format(time.RFC822)))
		})
	}
}

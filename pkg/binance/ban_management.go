package binance

import (
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/pkg/log"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	banned    atomic.Bool
	endOfBanS atomic.Int64
	logger    *zap.Logger
)

func InitLogger() {
	logger = log.GetLogger("Ban manager")
}

func isBanned() bool {
	if banned.Load() == true {
		if time.Now().Unix() < endOfBanS.Load() {
			return true
		}
		banned.Store(false)
	}
	return false
}

func banBinanceRequests(response *http.Response, e error) error {
	timeoutValue := response.Header.Get("Retry-After")
	if timeoutValue == "" {
		logger.Warn(InvalidBinanceDataErr.Error())
		return InvalidBinanceDataErr
	}
	timeout, err := strconv.ParseInt(timeoutValue, 10, 64)
	if err != nil {
		logger.Warn(InvalidBinanceDataErr.Error())
		return InvalidBinanceDataErr
	}
	logger.Error(e.Error())
	setBanned(timeout)
	return e
}

func setBanned(banTimestamp int64) {
	banned.Store(true)
	endOfBanS.Store(time.Now().Unix() + banTimestamp)
}

var RequestRejectedErr = fmt.Errorf("attempt of sending request while weight limit exceeded%w", svc.StopRetryErr)

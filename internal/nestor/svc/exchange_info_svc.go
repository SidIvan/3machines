package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ExchangeInfoSvc struct {
	logger          *zap.Logger
	binanceClient   BinanceClient
	dataStorages    []BatchedDataStorage[model.ExchangeInfo]
	exInfoUpdPeriod time.Duration
	shutdown        *atomic.Bool
	done            chan struct{}
	exInfoCache     *cache.ExchangeInfoCache
}

func NewExchangeInfoSvc(exInfoUpdPeriod time.Duration, binanceClient BinanceClient, dataStorages []BatchedDataStorage[model.ExchangeInfo], infoCache *cache.ExchangeInfoCache) *ExchangeInfoSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &ExchangeInfoSvc{
		logger:          log.GetLogger("ExchangeInfoSvc"),
		binanceClient:   binanceClient,
		dataStorages:    dataStorages,
		exInfoUpdPeriod: exInfoUpdPeriod,
		shutdown:        &shutdown,
		done:            make(chan struct{}),
		exInfoCache:     infoCache,
	}
}

func (s *ExchangeInfoSvc) StartReceiveExInfo(ctx context.Context) {
	for {
		if s.shutdown.Load() {
			s.done <- struct{}{}
			return
		}
		time.Sleep(s.exInfoUpdPeriod)
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		exInfo, err := s.binanceClient.GetFullExchangeInfo(ctxWithTimeout)
		cancel()
		if err == nil {
			s.logger.Info(fmt.Sprintf("got exchange info with hash %d", exInfo.ExInfoHash()))
		} else {
			s.logger.Warn("error when getting exchange info, sleep")
			continue
		}
		ctxWithTimeout, cancel = context.WithTimeout(context.Background(), 20*time.Second)
		lastSavedExInfo := s.dataStorages[0].(ExchangeInfoStorage).GetLastExchangeInfo(ctxWithTimeout)
		cancel()
		if lastSavedExInfo == nil {
			s.logger.Error("error while receiving last saved exchange info from clickhouse")
		} else if !model.EqualsExchangeInfos(lastSavedExInfo, exInfo) {
			s.logger.Info("exchange info changed, attempt to send")
			if err = s.saveExInfo(ctx, []model.ExchangeInfo{*model.NewExchangeInfo(exInfo)}); err != nil {
				s.logger.Error(err.Error())
			}
		} else {
			s.logger.Info("exchange info did not change")
		}
	}
}

func (s *ExchangeInfoSvc) saveExInfo(ctx context.Context, exInfo []model.ExchangeInfo) error {
	for i, storage := range s.dataStorages {
		for j := 0; j < 3; j++ {
			err := storage.Save(ctx, exInfo)
			if err == nil {
				if i > 0 {
					s.logger.Warn(fmt.Sprintf("data saved to additional storage with no = %d", i))
				}
				return nil
			}
		}
	}
	return ErrNotSaved
}

func (s *ExchangeInfoSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	<-s.done
	s.logger.Info("successfully shutdown")
}

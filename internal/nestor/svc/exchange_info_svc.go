package svc

import (
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ExchangeInfoSvc struct {
	logger          *zap.Logger
	binanceClient   BinanceClient
	localRepo       LocalRepo
	exInfoStorage   ExchangeInfoStorage
	metrics         MetricsHolder
	cfg             *conf.AppConfig
	shutdown        *atomic.Bool
	done            chan struct{}
	exInfoCache     *cache.ExchangeInfoCache
	useLocalStorage bool
}

func NewExchangeInfoSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, exInfoStorage ExchangeInfoStorage, metrics MetricsHolder, infoCache *cache.ExchangeInfoCache, useLocalStorage bool) *ExchangeInfoSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &ExchangeInfoSvc{
		logger:          log.GetLogger("ExchangeInfoSvc"),
		binanceClient:   binanceClient,
		metrics:         metrics,
		localRepo:       localRepo,
		exInfoStorage:   exInfoStorage,
		cfg:             config,
		shutdown:        &shutdown,
		done:            make(chan struct{}),
		exInfoCache:     infoCache,
		useLocalStorage: useLocalStorage,
	}
}

func (s *ExchangeInfoSvc) StartReceiveExInfo(ctx context.Context) {
	for {
		if s.shutdown.Load() {
			s.done <- struct{}{}
			return
		}
		time.Sleep(time.Duration(s.cfg.ExchangeInfoUpdPerM) * time.Minute)
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		exInfo, err := s.binanceClient.GetFullExchangeInfo(ctxWithTimeout)
		cancel()
		if err == nil {
			s.metrics.UpdateMetrics(exInfo.Symbols)
			s.metrics.ProcessExInfoMetrics(Receive)
			s.logger.Info(fmt.Sprintf("got exchange info with hash %d", exInfo.ExInfoHash()))
		} else {
			s.logger.Warn("error when getting exchange info, sleep")
			continue
		}
		ctxWithTimeout, cancel = context.WithTimeout(context.Background(), 20*time.Second)
		lastSavedExInfo := s.exInfoStorage.GetLastExchangeInfo(ctxWithTimeout)
		cancel()
		if err != nil {
			s.logger.Error(err.Error())
		} else if lastSavedExInfo == nil {
			s.logger.Error("error while receiving last saved exchange info from clickhouse")
		} else if !bmodel.EqualsExchangeInfos(lastSavedExInfo, exInfo) {
			s.logger.Info("exchange info changed, attempt to send")
			if err = s.SaveExchangeInfo(ctx, exInfo); err != nil {
				s.logger.Error(err.Error())
			}
		} else {
			s.logger.Info("exchange info did not change")
		}
	}
}

func (s *ExchangeInfoSvc) SaveExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) error {
	s.logger.Info("sending exchange info")
	var err error
	for i := 0; i < 3; i++ {
		if err = s.exInfoStorage.SendExchangeInfo(ctx, exInfo); err == nil {
			s.logger.Info("successfully sent to Ch")
			s.metrics.ProcessExInfoMetrics(Send)
			return nil
		} else {
			s.logger.Warn("failed send to Ch, retry")
			//s.globalRepo.Reconnect(ctx)
		}
	}
	if !s.useLocalStorage {
		s.logger.Debug("cannot use local storage")
		return err
	}
	s.exInfoStorage.Reconnect(ctx)
	s.logger.Warn("failed send to Ch, try save to mongo")
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveExchangeInfo(ctx, exInfo); err == nil {
			s.metrics.ProcessExInfoMetrics(Save)
			s.logger.Info("successfully saved to mongo")
			return nil
		}
		s.logger.Warn("failed save to mongo, retry")
	}
	s.logger.Warn("failed save to mongo, attempting save to file")
	// УСЁ ПРОПАЛО
	return s.saveExchangeInfoToFile(exInfo)
}

func (s *ExchangeInfoSvc) saveExchangeInfoToFile(ticks *bmodel.ExchangeInfo) error {
	file, err := os.Create("exchange_info" + strconv.FormatInt(time.Now().UnixMilli(), 10))
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	data, _ := json.Marshal(ticks)
	if _, err = file.Write(data); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	file.Close()
	return nil
}

func (s *ExchangeInfoSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	<-s.done
	s.logger.Info("successfully shutdown")
}

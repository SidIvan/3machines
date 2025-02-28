package svc

import (
	"DeltaReceiver/internal/common/model"
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
	dataStorages    []BatchedDataStorage[model.ExchangeInfo]
	metrics         MetricsHolder
	cfg             *conf.AppConfig
	shutdown        *atomic.Bool
	done            chan struct{}
	exInfoCache     *cache.ExchangeInfoCache
	useLocalStorage bool
}

func NewExchangeInfoSvc(config *conf.AppConfig, binanceClient BinanceClient, dataStorages []BatchedDataStorage[model.ExchangeInfo], metrics MetricsHolder, infoCache *cache.ExchangeInfoCache) *ExchangeInfoSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &ExchangeInfoSvc{
		logger:        log.GetLogger("ExchangeInfoSvc"),
		binanceClient: binanceClient,
		metrics:       metrics,
		dataStorages:  dataStorages,
		cfg:           config,
		shutdown:      &shutdown,
		done:          make(chan struct{}),
		exInfoCache:   infoCache,
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
		lastSavedExInfo := s.dataStorages[0].(ExchangeInfoStorage).GetLastExchangeInfo(ctxWithTimeout)
		cancel()
		if err != nil {
			s.logger.Error(err.Error())
		} else if lastSavedExInfo == nil {
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

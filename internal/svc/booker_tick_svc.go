package svc

import (
	"DeltaReceiver/internal/conf"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type BookTickerSvc struct {
	logger        *zap.Logger
	binanceClient BinanceClient
	localRepo     LocalRepo
	globalRepo    GlobalRepo
	metricsHolder MetricsHolder
	cfg           *conf.AppConfig
	shutdown      *atomic.Bool
}

func NewBookerTickSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, globalRepo GlobalRepo, metricsHolder MetricsHolder) *BookTickerSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &BookTickerSvc{
		logger:        log.GetLogger("BookTickerSvc"),
		binanceClient: binanceClient,
		metricsHolder: metricsHolder,
		localRepo:     localRepo,
		globalRepo:    globalRepo,
		cfg:           config,
		shutdown:      &shutdown,
	}
}

func (s *BookTickerSvc) StartReceiveOrderBooksTops(ctx context.Context) {
	for {
		if s.shutdown.Load() {
			return
		}
		if ticks, err := s.binanceClient.GetBookTicks(ctx); err != nil {
			s.logger.Error(err.Error())
		} else {
			err = s.SaveTicks(ctx, ticks)
			if err != nil {
				s.logger.Error(err.Error())
			}
		}
		time.Sleep(time.Minute * 1)
	}
}

func (s *BookTickerSvc) SaveTicks(ctx context.Context, ticks []bmodel.SymbolTick) error {
	s.logger.Info("sending bock ticker")
	for i := 0; i < 3; i++ {
		if err := s.globalRepo.SendBookTicks(ctx, ticks); err == nil {
			s.logger.Info("successfully sended to Ch")
			return nil
		} else {
			s.logger.Warn("failed send to Ch, retry")
			s.globalRepo.Reconnect(ctx)
		}
	}
	s.logger.Warn("failed send to Ch, try save to mongo")
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveBookTicker(ctx, ticks); err == nil {
			s.logger.Info("successfully saved to mongo")
			return nil
		}
		s.logger.Warn("failed save to mongo, retry")
	}
	s.logger.Warn("failed save to mongo, attempting save to file")
	// УСЁ ПРОПАЛО
	return s.saveTicksToFile(ticks)
}

func (s *BookTickerSvc) saveTicksToFile(deltas []bmodel.SymbolTick) error {
	file, err := os.Create("ticks" + strconv.FormatInt(time.Now().UnixMilli(), 10))
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	data, _ := json.Marshal(deltas)
	if _, err = file.Write(data); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	file.Close()
	return nil
}

func (s *BookTickerSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
}

package svc

import (
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type DeltaReceiverSvc struct {
	logger               *zap.Logger
	binanceClient        BinanceClient
	deltaReceivers       []*DeltaReceiver
	localRepo            LocalRepo
	deltaStorage         DeltaStorage
	metricsHolder        MetricsHolder
	cfg                  *conf.AppConfig
	shutdown             *atomic.Bool
	exInfoCache          *cache.ExchangeInfoCache
	deltaUpdateIdWatcher *cache.DeltaUpdateIdWatcher
	deltaHolesStorage    DeltaHolesStorage
}

func NewDeltaReceiverSvc(
	config *conf.AppConfig,
	binanceClient BinanceClient,
	localRepo LocalRepo,
	deltaStorage DeltaStorage,
	metricsHolder MetricsHolder,
	exInfoCache *cache.ExchangeInfoCache,
	deltaUpdateIdWatcher *cache.DeltaUpdateIdWatcher,
	deltaHolesStorage DeltaHolesStorage) *DeltaReceiverSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &DeltaReceiverSvc{
		logger:               log.GetLogger("DeltaReceiverSvc"),
		binanceClient:        binanceClient,
		metricsHolder:        metricsHolder,
		localRepo:            localRepo,
		deltaStorage:         deltaStorage,
		cfg:                  config,
		shutdown:             &shutdown,
		exInfoCache:          exInfoCache,
		deltaUpdateIdWatcher: deltaUpdateIdWatcher,
		deltaHolesStorage:    deltaHolesStorage,
	}
}

func (s *DeltaReceiverSvc) getAndActivateNewReceivers(ctx context.Context) []*DeltaReceiver {
	if s.shutdown.Load() {
		return nil
	}
	var symbols []string
	for _, symbolInfo := range s.exInfoCache.GetVal().Symbols {
		if symbolInfo.Status == "TRADING" {
			symbols = append(symbols, strings.ToLower(symbolInfo.Symbol))
		}
	}
	s.logger.Info(fmt.Sprintf("start get deltas of %d different symbols", len(symbols)))
	var newReceivers []*DeltaReceiver
	numReceivers := 30
	for i := 0; i < numReceivers; i++ {
		var symbolsForReceiver []string
		for j := 0; j*numReceivers+i < len(symbols); j++ {
			symbolsForReceiver = append(symbolsForReceiver, symbols[j*numReceivers+i])
		}
		if newReceiver := NewDeltaReceiver(s.cfg.BinanceHttpConfig, symbolsForReceiver, s.localRepo, s.deltaStorage, s.metricsHolder, s.deltaUpdateIdWatcher, s.deltaHolesStorage); newReceiver != nil {
			newReceivers = append(newReceivers, newReceiver)
		}
	}
	if (len(symbols) >= numReceivers && len(newReceivers) < numReceivers) || (len(symbols) < numReceivers && len(newReceivers) < len(symbols)) {
		s.logger.Error("not enough receivers")
		return nil
	}
	for _, receiver := range newReceivers {
		for k := 0; k < 3; k++ {
			if err := receiver.StartReceiveDeltas(ctx); err == nil {
				break
			} else {
				s.logger.Error(err.Error())
			}
		}
	}
	return newReceivers
}

func (s *DeltaReceiverSvc) gracefullyReconnectReceivers(ctx context.Context) {
	newReceivers := s.getAndActivateNewReceivers(ctx)
	for _, receiver := range s.deltaReceivers {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		receiver.Shutdown(ctxWithTimeout)
		cancel()
	}
	s.deltaReceivers = newReceivers
}

func (s *DeltaReceiverSvc) ReceiveDeltasPairs(ctx context.Context) {
	s.deltaReceivers = s.getAndActivateNewReceivers(ctx)
	for {
		time.Sleep(time.Duration(s.cfg.ReconnectPeriodM) * time.Minute)
		s.gracefullyReconnectReceivers(ctx)
	}
}

func (s *DeltaReceiverSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.logger.Debug(fmt.Sprintf("need to shutdown %d receivers", len(s.deltaReceivers)))
	for _, receiver := range s.deltaReceivers {
		receiver.Shutdown(ctx)
	}
	s.logger.Info("successfully shutdown")
}

package svc

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DeltaReceiverSvc struct {
	logger         *zap.Logger
	binanceClient  BinanceClient
	deltaReceivers []*DeltaReceiver
	localRepo      LocalRepo
	globalRepo     GlobalRepo
	metricsHolder  MetricsHolder
	cfg            *conf.AppConfig
	shutdown       *atomic.Bool
	dRecWg         *sync.WaitGroup
	exInfoCache    *cache.ExchangeInfoCache
}

func NewDeltaReceiverSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, globalRepo GlobalRepo, metricsHolder MetricsHolder, exInfoCache *cache.ExchangeInfoCache) *DeltaReceiverSvc {
	var shutdown atomic.Bool
	var dRecWg sync.WaitGroup
	shutdown.Store(false)
	return &DeltaReceiverSvc{
		logger:        log.GetLogger("DeltaReceiverSvc"),
		binanceClient: binanceClient,
		metricsHolder: metricsHolder,
		localRepo:     localRepo,
		globalRepo:    globalRepo,
		cfg:           config,
		shutdown:      &shutdown,
		dRecWg:        &dRecWg,
		exInfoCache:   exInfoCache,
	}
}

func (s *DeltaReceiverSvc) getNewReceivers(ctx context.Context) []*DeltaReceiver {
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
	for i := 0; i < len(symbols)/40; i++ {
		var symbolsForReceiver []string
		for j := 0; j*40+i < len(symbols); j++ {
			symbolsForReceiver = append(symbolsForReceiver, symbols[j*40+i])
		}
		if newReceiver := NewDeltaReceiver(s.cfg.BinanceHttpConfig, symbolsForReceiver, s.localRepo, s.globalRepo); newReceiver != nil {
			newReceivers = append(newReceivers, newReceiver)
		}
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

func (s *DeltaReceiverSvc) grasefullyReconnectReceivers(ctx context.Context) {
	newReceivers := s.getNewReceivers(ctx)
	for _, receiver := range s.deltaReceivers {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		receiver.Shutdown(ctxWithTimeout)
		cancel()
	}
	s.deltaReceivers = newReceivers
}

func (s *DeltaReceiverSvc) ReceiveDeltasPairs(ctx context.Context) {
	s.deltaReceivers = s.getNewReceivers(ctx)
	for {
		time.Sleep(time.Duration(s.cfg.ReconnectPeriodM) * time.Minute)
		s.grasefullyReconnectReceivers(ctx)
	}
}

func (s *DeltaReceiverSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	for _, receiver := range s.deltaReceivers {
		receiver.Shutdown(ctx)
	}
}

package svc

import (
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type BookTickerSvc struct {
	logger           *zap.Logger
	binanceClient    BinanceClient
	tickerReceivers  []*TickerReceiver
	localRepo        LocalRepo
	bookTicksStorage svc.BookTicksStorage
	metrics          MetricsHolder
	cfg              *conf.AppConfig
	shutdown         *atomic.Bool
	dRecWg           *sync.WaitGroup
	exInfoCache      *cache.ExchangeInfoCache
}

func NewBookTickerSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, bookTicksStorage svc.BookTicksStorage, metrics MetricsHolder, exInfoCache *cache.ExchangeInfoCache) *BookTickerSvc {
	var shutdown atomic.Bool
	var dRecWg sync.WaitGroup
	shutdown.Store(false)
	return &BookTickerSvc{
		logger:           log.GetLogger("BookTickerSvc"),
		binanceClient:    binanceClient,
		metrics:          metrics,
		localRepo:        localRepo,
		bookTicksStorage: bookTicksStorage,
		cfg:              config,
		shutdown:         &shutdown,
		dRecWg:           &dRecWg,
		exInfoCache:      exInfoCache,
	}
}

func (s *BookTickerSvc) getNewReceivers(ctx context.Context) []*TickerReceiver {
	if s.shutdown.Load() {
		return nil
	}
	var symbols []string
	for _, symbolInfo := range s.exInfoCache.GetVal().Symbols {
		if symbolInfo.Status == "TRADING" {
			symbols = append(symbols, strings.ToLower(symbolInfo.Symbol))
		}
	}
	s.logger.Info(fmt.Sprintf("start get ticks of %d different symbols", len(symbols)))
	var newReceivers []*TickerReceiver
	numTickerReceivers := 5
	for i := 0; i < numTickerReceivers; i++ {
		var symbolsForReceiver []string
		for j := 0; j*numTickerReceivers+i < len(symbols); j++ {
			symbolsForReceiver = append(symbolsForReceiver, symbols[j*numTickerReceivers+i])
		}
		if newReceiver := NewTickerReceiver(s.cfg.BinanceHttpConfig, symbolsForReceiver, s.localRepo, s.bookTicksStorage, s.metrics); newReceiver != nil {
			newReceivers = append(newReceivers, newReceiver)
		}
	}
	for _, receiver := range newReceivers {
		for k := 0; k < 3; k++ {
			if err := receiver.StartReceiveTicks(ctx); err == nil {
				break
			} else {
				s.logger.Error(err.Error())
			}
		}
	}
	return newReceivers
}

func (s *BookTickerSvc) grasefullyReconnectReceivers(ctx context.Context) {
	newReceivers := s.getNewReceivers(ctx)
	for _, receiver := range s.tickerReceivers {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		receiver.Shutdown(ctxWithTimeout)
		cancel()
	}
	s.tickerReceivers = newReceivers
}

func (s *BookTickerSvc) StartReceiveOrderBooksTops(ctx context.Context) {
	s.tickerReceivers = s.getNewReceivers(ctx)
	for {
		time.Sleep(time.Duration(s.cfg.ReconnectPeriodM) * time.Minute)
		s.grasefullyReconnectReceivers(ctx)
	}
}

func (s *BookTickerSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.logger.Debug(fmt.Sprintf("need to shutdown %d receivers", len(s.tickerReceivers)))
	for _, receiver := range s.tickerReceivers {
		receiver.Shutdown(ctx)
	}
	s.logger.Info("successfully shutdown")
}

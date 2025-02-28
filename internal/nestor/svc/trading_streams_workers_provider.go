package svc

import (
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type TradingSymbolsWorkersProvider[T any] struct {
	logger         *zap.Logger
	numWorkers     int
	workerProvider TradingSymbolsWorkerProvider[T]
	exInfoCache    *cache.ExchangeInfoCache
}

func NewTradingSymbolsWorkersProvider[T any](workersProviderType string, numWorkers int, workerProvider TradingSymbolsWorkerProvider[T], exInfoCache *cache.ExchangeInfoCache) *TradingSymbolsWorkersProvider[T] {
	return &TradingSymbolsWorkersProvider[T]{
		logger:         log.GetLogger(fmt.Sprintf("TradingSymbolsWorkersProvider[%s]", workersProviderType)),
		numWorkers:     numWorkers,
		workerProvider: workerProvider,
		exInfoCache:    exInfoCache,
	}
}

func (s *TradingSymbolsWorkersProvider[T]) getNewWorkers(ctx context.Context) []*T {
	var symbols []string
	for _, symbolInfo := range s.exInfoCache.GetVal().Symbols {
		if symbolInfo.Status == "TRADING" {
			symbols = append(symbols, strings.ToLower(symbolInfo.Symbol))
		}
	}
	s.logger.Info(fmt.Sprintf("start construct workers of %d different symbols", len(symbols)))
	newWorkers := make([]*T, s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		var symbolsForWorker []string
		for j := 0; j*s.numWorkers+i < len(symbols); j++ {
			symbolsForWorker = append(symbolsForWorker, symbols[j*s.numWorkers+i])
		}
		newWorkers[i] = s.workerProvider.getNewWorkers(ctx, symbolsForWorker)
	}
	return newWorkers
}

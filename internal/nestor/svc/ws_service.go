package svc

import (
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type WsSvc[TRecv, TResp any] struct {
	logger          *zap.Logger
	workersProvider WsDataWorkersProvider[WsDataProcessWorker[TRecv, TResp]]
	workers         []*WsDataProcessWorker[TRecv, TResp]
	dataStorages    []BatchedDataStorage[TResp]
	metrics         WsDataPipelineMetrics[TResp]
	reconnectPeriod time.Duration
	exInfoCache     *cache.ExchangeInfoCache
	shutdown        *atomic.Bool
}

func NewWsSvc[TRecv, TResp any](
	dataType string,
	workersProvider WsDataWorkersProvider[WsDataProcessWorker[TRecv, TResp]],
	dataStorages []BatchedDataStorage[TResp],
	metrics WsDataPipelineMetrics[TResp],
	reconnectPeriod time.Duration,
	exInfoCache *cache.ExchangeInfoCache) *WsSvc[TRecv, TResp] {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &WsSvc[TRecv, TResp]{
		logger:          log.GetLogger(fmt.Sprintf("WsSvc[%s]", dataType)),
		workersProvider: workersProvider,
		dataStorages:    dataStorages,
		metrics:         metrics,
		reconnectPeriod: reconnectPeriod,
		exInfoCache:     exInfoCache,
		shutdown:        &shutdown,
	}
}

func (s *WsSvc[TRecv, TResp]) Start(ctx context.Context) {
	s.workers = s.getAndActivateNewWorkers(ctx)
	for {
		time.Sleep(s.reconnectPeriod)
		s.updateWorkers(ctx)
	}
}

func (s *WsSvc[TRecv, TResp]) getAndActivateNewWorkers(ctx context.Context) []*WsDataProcessWorker[TRecv, TResp] {
	if s.shutdown.Load() {
		return nil
	}
	newWorkers := s.workersProvider.GetNewWorkers(ctx)
	for _, worker := range newWorkers {
		for k := 0; k < 3; k++ {
			if err := worker.Start(ctx); err == nil {
				break
			} else {
				s.logger.Error(err.Error())
			}
		}
	}
	return newWorkers
}

func (s *WsSvc[TRecv, TResp]) updateWorkers(ctx context.Context) {
	newWorkers := s.getAndActivateNewWorkers(ctx)
	oldWorkers := s.workers
	s.workers = newWorkers
	for _, worker := range oldWorkers {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		worker.Shutdown(ctxWithTimeout)
		cancel()
	}
}

func (s *WsSvc[TRecv, TResp]) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.logger.Debug(fmt.Sprintf("need to shutdown %d workers", len(s.workers)))
	var wg sync.WaitGroup
	wg.Add(len(s.workers))
	for _, worker := range s.workers {
		go func(ctx context.Context) {
			defer wg.Done()
			worker.Shutdown(ctx)
		}(ctx)
	}
	wg.Done()
	s.logger.Info("successfully shutdown")
}

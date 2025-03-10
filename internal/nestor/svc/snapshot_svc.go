package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type SnapshotSvc struct {
	logger            *zap.Logger
	binanceClient     BinanceClient
	snapshotQueue     []string
	snapshotSchedules map[string]time.Time
	dataStorages      []BatchedDataStorage[model.DepthSnapshotPart]
	shutdown          *atomic.Bool
	done              chan struct{}
	exInfoCache       *cache.ExchangeInfoCache
	snapshotDepth     int
}

func NewSnapshotSvc(dataType string, snapshotDepth int, binanceClient BinanceClient, dataStorages []BatchedDataStorage[model.DepthSnapshotPart], infoCache *cache.ExchangeInfoCache) *SnapshotSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &SnapshotSvc{
		logger:            log.GetLogger(fmt.Sprintf("SnapshotSvc[%s]", dataType)),
		binanceClient:     binanceClient,
		dataStorages:      dataStorages,
		snapshotSchedules: make(map[string]time.Time),
		shutdown:          &shutdown,
		done:              make(chan struct{}),
		exInfoCache:       infoCache,
		snapshotDepth:     snapshotDepth,
	}
}

func (s *SnapshotSvc) StartReceiveAndSaveSnapshots(ctx context.Context) {
	for {
		s.snapshotQueue = nil
		tradingSymbols := s.exInfoCache.GetTradingSymbols()
		curTime := time.Now()
		s.logger.Info(fmt.Sprintf("start updating scheduling map, %d snapshots scheduled now", len(s.snapshotSchedules)))
		for _, symbol := range tradingSymbols {
			timeToGetSnapshot, ok := s.snapshotSchedules[symbol]
			if !ok {
				s.snapshotQueue = append(s.snapshotQueue, symbol)
			} else if timeToGetSnapshot.Before(curTime) {
				s.snapshotQueue = append(s.snapshotQueue, symbol)
				delete(s.snapshotSchedules, symbol)
			}
		}
		s.logger.Info(fmt.Sprintf("end updating scheduling map, %d snapshots scheduled now", len(s.snapshotSchedules)))
		if len(s.snapshotQueue) == 0 {
			time.Sleep(10 * time.Minute)
			continue
		}
		s.logger.Info(fmt.Sprintf("start of getting %d snapshots", len(s.snapshotQueue)))
		requestWeightLimit := s.exInfoCache.GetRequestWeightLimit()
		for _, symbol := range s.snapshotQueue {
			if s.shutdown.Load() {
				s.done <- struct{}{}
				return
			}
			limit, _ := s.ReceiveAndSaveSnapshot(ctx, symbol)
			s.logger.Debug(fmt.Sprintf("current limit is %s when allowed %d", limit, requestWeightLimit))
			if tmp, _ := strconv.Atoi(limit); limit != "" && tmp*10 > requestWeightLimit*8 {
				sleepTime := s.exInfoCache.GetRequestWeightLimitDuration()
				s.logger.Debug(fmt.Sprintf("sleeping snapshot service for %s", sleepTime))
				time.Sleep(sleepTime)
			}
		}
	}
}

func (s *SnapshotSvc) ReceiveAndSaveSnapshot(ctx context.Context, symbol string) (string, error) {
	snapshot, limit, err := s.binanceClient.GetFullSnapshot(ctx, symbol, s.snapshotDepth)
	defer func(err error) {
		if err != nil {
			s.logger.Error(fmt.Errorf("error while getting snapshot %s because of %w", symbol, err).Error())
			s.snapshotSchedules[symbol] = time.Now().Add(10 * time.Minute)
		}
		if len(snapshot) < 10000 {
			s.snapshotSchedules[symbol] = time.Now().Add(5 * 24 * time.Hour)
		} else {
			s.snapshotSchedules[symbol] = time.Now().Add(24 * time.Hour)
		}
	}(err)
	if err != nil {
		return limit, err
	}
	if len(snapshot) == 0 {
		s.logger.Warn("empty snapshot")
		return limit, nil
	}
	return limit, s.saveSnapshot(ctx, snapshot)
}

func (s *SnapshotSvc) saveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	for i, storage := range s.dataStorages {
		for j := 0; j < 3; j++ {
			err := storage.Save(ctx, snapshot)
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

func (s *SnapshotSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	<-s.done
	s.logger.Info("successfully shutdown")
}

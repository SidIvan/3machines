package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
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

type SnapshotSvc struct {
	logger            *zap.Logger
	binanceClient     BinanceClient
	snapshotQueue     []string
	snapshotSchedules map[string]time.Time
	localRepo         LocalRepo
	snapshotStorage   SnapshotStorage
	metrics           MetricsHolder
	cfg               *conf.AppConfig
	shutdown          *atomic.Bool
	done              chan struct{}
	exInfoCache       *cache.ExchangeInfoCache
}

func NewSnapshotSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, snapshotStorage SnapshotStorage, metricsHolder MetricsHolder, infoCache *cache.ExchangeInfoCache) *SnapshotSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &SnapshotSvc{
		logger:            log.GetLogger("SnapshotSvc"),
		binanceClient:     binanceClient,
		metrics:           metricsHolder,
		localRepo:         localRepo,
		snapshotStorage:   snapshotStorage,
		snapshotSchedules: make(map[string]time.Time),
		cfg:               config,
		shutdown:          &shutdown,
		done:              make(chan struct{}),
		exInfoCache:       infoCache,
	}
}

func (s *SnapshotSvc) StartReceiveAndSaveSnapshots(ctx context.Context) {
	for {
		s.snapshotQueue = nil
		exInfo := s.exInfoCache.GetVal()
		curTime := time.Now()
		s.logger.Info(fmt.Sprintf("start updating scheduling map, %d snapshots scheduled now", len(s.snapshotSchedules)))
		for _, symbolInfo := range exInfo.Symbols {
			if symbolInfo.Status != "TRADING" {
				continue
			}
			symbol := symbolInfo.Symbol
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
		for _, symbol := range s.snapshotQueue {
			if s.shutdown.Load() {
				s.done <- struct{}{}
				return
			}
			limit, _ := s.ReceiveAndSaveSnapshot(ctx, symbol)
			s.logger.Debug(fmt.Sprintf("current limit is %s when allowed %d", limit, exInfo.GetRequestWeightLimit()))
			if tmp, _ := strconv.Atoi(limit); limit != "" && tmp*10 > exInfo.GetRequestWeightLimit()*8 {
				sleepTime := exInfo.GetRequestWeightLimitDuration()
				s.logger.Debug(fmt.Sprintf("sleeping snapshot service for %s", sleepTime))
				time.Sleep(sleepTime)
			}
		}
	}
}

func (s *SnapshotSvc) ReceiveAndSaveSnapshot(ctx context.Context, symbol string) (string, error) {
	snapshot, limit, err := s.binanceClient.GetFullSnapshot(ctx, symbol, 5000)
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
	s.metrics.ProcessSnapshotMetrics(snapshot, Receive)
	if len(snapshot) == 0 {
		s.logger.Warn("empty snapshot")
		return limit, nil
	}
	err = s.saveSnapshotToGlobalRepo(ctx, snapshot)
	if err == nil {
		return limit, nil
	}
	err = s.saveSnapshotToLocalRepo(ctx, snapshot)
	if err == nil {
		return limit, nil
	}
	// УСЁ ПРОПАЛО
	return limit, s.saveSnapshotToFile(snapshot)
}

func (s *SnapshotSvc) saveSnapshotToGlobalRepo(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	var err error
	for i := 0; i < 3; i++ {
		if err := s.snapshotStorage.SendSnapshot(ctx, snapshot); err == nil {
			s.metrics.ProcessSnapshotMetrics(snapshot, Send)
			return nil
		} else {
			s.logger.Error(err.Error())
		}
	}
	return err
}

func (s *SnapshotSvc) saveSnapshotToLocalRepo(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	var err error
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveSnapshot(ctx, snapshot); err == nil {
			s.metrics.ProcessSnapshotMetrics(snapshot, Save)
			return nil
		} else {
			s.logger.Error(err.Error())
		}
	}
	return err
}

func (s *SnapshotSvc) saveSnapshotToFile(snapshot []model.DepthSnapshotPart) error {
	if file, err := os.Create("snapshot" + string(rune(time.Now().UnixMilli()))); err != nil {
		return err
	} else if data, err := json.Marshal(snapshot); err != nil {
		return err
	} else if _, err := file.Write(data); err != nil {
		return err
	} else {
		file.Close()
		return nil
	}
}

func (s *SnapshotSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	<-s.done
	s.logger.Info("successfully shutdown")
}

package svc

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type SnapshotSvc struct {
	logger            *zap.Logger
	binanceClient     BinanceClient
	snapshotQueue     []string
	snapshotSchedules map[string]time.Time
	localRepo         LocalRepo
	globalRepo        GlobalRepo
	metricsHolder     MetricsHolder
	cfg               *conf.AppConfig
	shutdown          *atomic.Bool
	done              chan struct{}
	exInfoCache       *cache.ExchangeInfoCache
}

func NewSnapshotSvc(config *conf.AppConfig, binanceClient BinanceClient, localRepo LocalRepo, globalRepo GlobalRepo, metricsHolder MetricsHolder, infoCache *cache.ExchangeInfoCache) *SnapshotSvc {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &SnapshotSvc{
		logger:            log.GetLogger("SnapshotSvc"),
		binanceClient:     binanceClient,
		metricsHolder:     metricsHolder,
		localRepo:         localRepo,
		globalRepo:        globalRepo,
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
	if err != nil {
		s.logger.Error(fmt.Errorf("error while getting snapshot %s because of %w", symbol, err).Error())
		s.snapshotSchedules[symbol] = time.Now().Add(10 * time.Minute)
		return limit, err
	}
	if err = s.SaveSnapshot(ctx, snapshot); err != nil {
		s.logger.Error(fmt.Sprintf("error while saving snapshot %s", symbol))
		s.snapshotSchedules[symbol] = time.Now().Add(10 * time.Minute)
		return limit, err
	}
	if len(snapshot) < 10000 {
		s.snapshotSchedules[symbol] = time.Now().Add(5 * 24 * time.Hour)
	} else {
		s.snapshotSchedules[symbol] = time.Now().Add(24 * time.Hour)
	}
	return limit, nil
}

func (s *SnapshotSvc) SaveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	if len(snapshot) == 0 {
		s.logger.Warn("empty snapshot")
		return nil
	}
	s.logger.Info(fmt.Sprintf("sending snapshot of %d parts [%s]", len(snapshot), snapshot[0].Symbol))
	for i := 0; i < 3; i++ {
		if err := s.globalRepo.SendSnapshot(ctx, snapshot); err == nil {
			s.logger.Info(fmt.Sprintf("successfully sent to Ch [%s]", snapshot[0].Symbol))
			return nil
		} else {
			s.logger.Error(err.Error())
			s.logger.Warn(fmt.Sprintf("failed send to Ch, try to reconnect [%s]", snapshot[0].Symbol))
		}
		//s.globalRepo.Reconnect(ctx)
	}
	s.globalRepo.Reconnect(ctx)
	s.logger.Warn(fmt.Sprintf("failed send to Ch, try save to mongo [%s]", snapshot[0].Symbol))
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveSnapshot(ctx, snapshot); err == nil {
			s.logger.Info(fmt.Sprintf("successfully saved to mongo [%s]", snapshot[0].Symbol))
			return nil
		}
		s.logger.Warn(fmt.Sprintf("failed save to mongo, try to reconnect [%s]", snapshot[0].Symbol))
		s.localRepo.Reconnect(ctx)
	}
	s.logger.Warn(fmt.Sprintf("failed save to mongo, attempting save to file [%s]", snapshot[0].Symbol))
	// УСЁ ПРОПАЛО
	return s.saveSnapshotToFile(snapshot)
}

func (s *SnapshotSvc) saveSnapshotToFile(snapshot []model.DepthSnapshotPart) error {
	if file, err := os.Create("snapshot" + string(time.Now().UnixMilli())); err != nil {
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

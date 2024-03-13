package svc

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type BinanceClient interface {
	GetFullSnapshot(ctx context.Context, pair string, depth int) ([]model.DepthSnapshotPart, error)
	GetFullExchangeInfo(context.Context) (*bmodel.ExchangeInfo, error)
}

type LocalRepo interface {
	GetLastSavedTimestamp(context.Context, model.Symbol) time.Time
	SaveDeltas(context.Context, []model.Delta) bool
	SaveSnapshot(context.Context, []model.DepthSnapshotPart) bool
	Reconnect(ctx context.Context)
}

type GlobalRepo interface {
	GetLastSavedTimestamp(context.Context, model.Symbol) time.Time
	SendDeltas(context.Context, []model.Delta) bool
	SendSnapshot(context.Context, []model.DepthSnapshotPart) bool
	Reconnect(ctx context.Context)
	SendFullExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) bool
}

type DeltaReceiver interface {
	ReceiveDeltas(ctx context.Context) []model.Delta
	Shutdown(context.Context)
	GetSymbol() model.Symbol
	Reconnect() bool
}

type MetricsHolder interface {
	IncreaseDeltaCtr(model.Symbol, int)
	IncreaseSnapshotPairCtr(model.Symbol, int)
	IncrementSuccessDeltaReconnectCtr(model.Symbol)
	IncrementFailedDeltaReconnectCtr(model.Symbol)
}

type DeltaReceiverSvc struct {
	log            *zap.Logger
	binanceClient  BinanceClient
	deltaReceivers []DeltaReceiver
	localRepo      LocalRepo
	globalRepo     GlobalRepo
	metricsHolder  MetricsHolder
	cfg            *conf.AppConfig
	shutdown       *atomic.Bool
	dRecWg         *sync.WaitGroup
	exInfoCache    *cache.ExchangeInfoCache
}

func NewDeltaReceiverSvc(config *conf.AppConfig, binanceClient BinanceClient, deltaReceivers []DeltaReceiver, localRepo LocalRepo,
	globalRepo GlobalRepo, metricsHolder MetricsHolder, exInfo *bmodel.ExchangeInfo) *DeltaReceiverSvc {
	var shutdown atomic.Bool
	var dRecWg sync.WaitGroup
	shutdown.Store(false)
	return &DeltaReceiverSvc{
		log:            log.GetLogger("DeltaReceiverSvc"),
		binanceClient:  binanceClient,
		deltaReceivers: deltaReceivers,
		localRepo:      localRepo,
		globalRepo:     globalRepo,
		metricsHolder:  metricsHolder,
		cfg:            config,
		shutdown:       &shutdown,
		dRecWg:         &dRecWg,
		exInfoCache:    cache.NewExchangeInfoCache(exInfo),
	}
}

func (s *DeltaReceiverSvc) CronExchangeInfoUpdatesStoring() {
	for {
		time.Sleep(time.Duration(s.cfg.ExchangeInfoUpdPerM) * time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		exInfo, err := s.binanceClient.GetFullExchangeInfo(ctx)
		cancel()
		if err != nil {
			s.log.Error(err.Error())
		} else if !bmodel.EqualsExchangeInfos(s.exInfoCache.GetVal(), exInfo) {
			for i := 0; i < 3; i++ {
				if s.globalRepo.SendFullExchangeInfo(ctx, exInfo) {
					s.log.Info(fmt.Sprintf("successfully sended exchange info to Ch "))
					break
				}
				s.log.Warn(fmt.Sprintf("failed send exchange info to Ch, try to reconnect"))
				s.globalRepo.Reconnect(ctx)
			}
			s.exInfoCache.SetVal(exInfo)
		}

	}
}

func (s *DeltaReceiverSvc) CronGetAndStoreFullSnapshot(pair string, periodM int16) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		s.GetAndStoreFullSnapshot(ctx, pair)
		cancel()
		time.Sleep(time.Duration(periodM) * time.Minute)
	}
}

func (s *DeltaReceiverSvc) GetAndStoreFullSnapshot(ctx context.Context, pair string) {
	snapshot, err := s.binanceClient.GetFullSnapshot(ctx, pair, s.cfg.FullSnapshotDepth)
	if err != nil {
		s.log.Error(err.Error())
		return
	}
	s.metricsHolder.IncreaseSnapshotPairCtr(model.SymbolFromString(pair), len(snapshot))
	s.sendSnapshot(ctx, snapshot)
}

func (s *DeltaReceiverSvc) ReceiveDeltasPairs() {
	for _, deltaReceiver := range s.deltaReceivers {
		go func(deltaReceiver DeltaReceiver) {
			for {
				if s.shutdown.Load() {
					return
				}
				s.dRecWg.Add(1)
				s.ReceivePair(deltaReceiver)
				s.dRecWg.Done()
			}
		}(deltaReceiver)
		go func(deltaReceiver DeltaReceiver) {
			for {
				if s.shutdown.Load() {
					return
				}
				if deltaReceiver.Reconnect() {
					s.metricsHolder.IncrementSuccessDeltaReconnectCtr(deltaReceiver.GetSymbol())
				} else {
					s.metricsHolder.IncrementFailedDeltaReconnectCtr(deltaReceiver.GetSymbol())
				}
			}
		}(deltaReceiver)
	}
}

func (s *DeltaReceiverSvc) ReceivePair(deltaReceiver DeltaReceiver) {
	s.log.Info(fmt.Sprintf("start receive deltas [%s]", deltaReceiver.GetSymbol()))
	ctx := context.Background()
	batchSize := s.cfg.GDBBatchSize
	var deltas []model.Delta
	for {
		receivedDeltas := deltaReceiver.ReceiveDeltas(ctx)
		s.metricsHolder.IncreaseDeltaCtr(deltaReceiver.GetSymbol(), len(deltas))
		if receivedDeltas == nil {
			s.log.Info(fmt.Sprintf("last batch of deltas [%s]", deltaReceiver.GetSymbol()))
			s.sendDeltas(ctx, deltas, deltaReceiver.GetSymbol())
			return
		}
		deltas = append(deltas, receivedDeltas...)
		if len(deltas) >= batchSize {
			s.log.Info(fmt.Sprintf("got full batch of deltas [%s]", deltaReceiver.GetSymbol()))
			s.sendDeltas(ctx, deltas, deltaReceiver.GetSymbol())
			deltas = make([]model.Delta, 0)
		}
	}
}

func (s *DeltaReceiverSvc) sendSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) {
	if len(snapshot) == 0 {
		s.log.Warn("empty snapshot")
		return
	}
	s.log.Info(fmt.Sprintf("sending snapshot of %d parts [%s]", len(snapshot), snapshot[0].Symbol))
	for i := 0; i < 3; i++ {
		if s.globalRepo.SendSnapshot(ctx, snapshot) {
			s.log.Info(fmt.Sprintf("successfully sended to Ch [%s]", snapshot[0].Symbol))
			return
		}
		s.log.Warn(fmt.Sprintf("failed send to Ch, try to reconnect [%s]", snapshot[0].Symbol))
		s.globalRepo.Reconnect(ctx)
	}
	s.log.Warn(fmt.Sprintf("failed send to Ch, try save to mongo [%s]", snapshot[0].Symbol))
	for i := 0; i < 3; i++ {
		if s.localRepo.SaveSnapshot(ctx, snapshot) {
			s.log.Info(fmt.Sprintf("successfully saved to mongo [%s]", snapshot[0].Symbol))
			return
		}
		s.log.Warn(fmt.Sprintf("failed save to mongo, try to reconnect [%s]", snapshot[0].Symbol))
		s.localRepo.Reconnect(ctx)
	}
	s.log.Warn(fmt.Sprintf("failed save to mongo, attempting save to file [%s]", snapshot[0].Symbol))
	// УСЁ ПРОПАЛО
	s.saveSnapshotToFile(snapshot)
	return
}

func (s *DeltaReceiverSvc) sendDeltas(ctx context.Context, deltas []model.Delta, symbol model.Symbol) {
	lastSavedTs := s.globalRepo.GetLastSavedTimestamp(context.Background(), symbol)
	for i := 0; i < len(deltas); {
		if lastSavedTs.UnixMilli() >= deltas[i].Timestamp {
			i++
			continue
		}
		curTime := time.Now().UnixMilli()
		s.log.Info(fmt.Sprintf("sending batch of %d deltas, send timestamp %d", len(deltas), curTime))
		for i := 0; i < 3; i++ {
			if s.globalRepo.SendDeltas(ctx, deltas) {
				s.log.Info(fmt.Sprintf("successfully sended to Ch, send timestamp %d", curTime))
				return
			}
			s.log.Warn(fmt.Sprintf("failed send to Ch, try to reconnect %d [%s]", curTime, symbol))
			s.globalRepo.Reconnect(ctx)
		}
		s.log.Warn(fmt.Sprintf("failed send to Ch, try save to mongo send timestamp %d [%s]", curTime, symbol))
		for i := 0; i < 3; i++ {
			if s.localRepo.SaveDeltas(ctx, deltas) {
				s.log.Info(fmt.Sprintf("successfully saved to mongo, send timestamp %d", curTime))
				return
			}
			s.log.Warn(fmt.Sprintf("failed save to mongo, try to reconnect timestamp %d", curTime))
			s.localRepo.Reconnect(ctx)
		}
		s.log.Warn(fmt.Sprintf("failed save to mongo, attempting save to file, send timestamp %d", curTime))
		// УСЁ ПРОПАЛО
		s.saveDeltasToFile(deltas)
		return
	}
	s.log.Info("empty deltas batch")
}

func (s *DeltaReceiverSvc) saveSnapshotToFile(snapshot []model.DepthSnapshotPart) {
	file, _ := os.Create("snapshot" + string(time.Now().UnixMilli()))
	data, _ := json.Marshal(snapshot)
	file.Write(data)
	file.Close()
}

func (s *DeltaReceiverSvc) saveDeltasToFile(deltas []model.Delta) {
	file, err := os.Create("deltas" + strconv.FormatInt(time.Now().UnixMilli(), 10))
	if err != nil {
		s.log.Error(err.Error())
		return
	}
	data, _ := json.Marshal(deltas)
	if _, err = file.Write(data); err != nil {
		s.log.Error(err.Error())
		return
	}
	file.Close()
}

func (s *DeltaReceiverSvc) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	for _, recv := range s.deltaReceivers {
		go recv.Shutdown(ctx)
	}
	s.dRecWg.Wait()
}

var (
	StopRetryErr = errors.New("")
)

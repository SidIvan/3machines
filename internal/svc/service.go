package svc

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
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
}

type DeltaReceiver interface {
	ReceiveDeltas(ctx context.Context) []model.Delta
	Shutdown(context.Context)
	GetSymbol() model.Symbol
	Reconnect()
}

type MetricsHolder interface {
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
}

func NewDeltaReceiverSvc(config *conf.AppConfig, binanceClient BinanceClient, deltaReceivers []DeltaReceiver, localRepo LocalRepo,
	globalRepo GlobalRepo, metricsHolder MetricsHolder) *DeltaReceiverSvc {
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
	}
}

const fullSnapshotDepth = 5000

func (s *DeltaReceiverSvc) CronGetAndStoreFullSnapshot(pair string, periodM int16) {
	for {
		time.Sleep(time.Duration(periodM) * time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s.GetAndStoreFullSnapshot(ctx, pair)
		cancel()
	}
}

func (s *DeltaReceiverSvc) GetAndStoreFullSnapshot(ctx context.Context, pair string) {
	snapshot, err := s.binanceClient.GetFullSnapshot(ctx, pair, fullSnapshotDepth)
	if err != nil {
		s.log.Error(err.Error())
		return
	}
	for i := 0; i < 3; i++ {
		if s.globalRepo.SendSnapshot(ctx, snapshot) {

		}
	}
	if !s.globalRepo.SendSnapshot(ctx, snapshot) {
		if s.localRepo.SaveSnapshot(ctx, snapshot) {
			return
		}
		// УСЁ ПРОПАЛО
		s.saveSnapshotToFile(snapshot)
	}
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
				deltaReceiver.Reconnect()
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
		if receivedDeltas == nil {
			s.log.Info(fmt.Sprintf("last batch of deltas [%s]", deltaReceiver.GetSymbol()))
			s.sendDeltas(ctx, deltas)
			return
		}
		for _, delta := range receivedDeltas {
			deltas = append(deltas, delta)
			if len(deltas) == batchSize {
				s.log.Info(fmt.Sprintf("got full batch of deltas [%s]", deltaReceiver.GetSymbol()))
				s.sendDeltas(ctx, deltas)
				deltas = make([]model.Delta, 0)
			}
		}
	}
}

func (s *DeltaReceiverSvc) sendDeltas(ctx context.Context, deltas []model.Delta) {
	curTime := time.Now().UnixMilli()
	s.log.Info(fmt.Sprintf("sending batch of %d deltas, send timestamp %d", len(deltas), curTime))
	for i := 0; i < 3; i++ {
		if s.globalRepo.SendDeltas(ctx, deltas) {
			s.log.Info(fmt.Sprintf("successfully sended to Ch, send timestamp %d", curTime))
			return
		}
		s.log.Warn(fmt.Sprintf("failed send to Ch, try to reconnect %d", curTime))
		s.globalRepo.Reconnect(ctx)
	}
	s.log.Warn(fmt.Sprintf("failed send to Ch, try save to mongo send timestamp %d", curTime))
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

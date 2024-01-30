package svc

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"errors"
	"go.uber.org/zap"
	"os"
	"time"
)

type BinanceClient interface {
	GetFullSnapshot(ctx context.Context, pair string, depth int) ([]model.DepthSnapshotPart, error)
}

type LocalRepo interface {
	SaveDeltas(context.Context, []model.Delta) bool
	SaveSnapshot(context.Context, []model.DepthSnapshotPart) bool
	Reconnect(ctx context.Context)
}

type GlobalRepo interface {
	SendDeltas(context.Context, []model.Delta) bool
	SendSnapshot(context.Context, []model.DepthSnapshotPart) bool
}

type DeltaReceiver interface {
	ReceiveDeltas(ctx context.Context, deltaCh chan<- model.Delta)
	Shutdown(context.Context)
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
}

func NewDeltaReceiverSvc(config *conf.AppConfig, binanceClient BinanceClient, deltaReceivers []DeltaReceiver, localRepo LocalRepo,
	globalRepo GlobalRepo, metricsHolder MetricsHolder) *DeltaReceiverSvc {
	return &DeltaReceiverSvc{
		log:            log.GetLogger("DeltaReceiverSvc"),
		binanceClient:  binanceClient,
		deltaReceivers: deltaReceivers,
		localRepo:      localRepo,
		globalRepo:     globalRepo,
		metricsHolder:  metricsHolder,
		cfg:            config,
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
				s.ReceivePair(deltaReceiver)
			}
		}(deltaReceiver)
	}
}

func (s *DeltaReceiverSvc) ReceivePair(deltaReceiver DeltaReceiver) {
	ctx := context.Background()
	deltaCh := make(chan model.Delta)
	go deltaReceiver.ReceiveDeltas(ctx, deltaCh)
	batchSize := s.cfg.GDBBatchSize
	for {
		deltas := make([]model.Delta, batchSize)
		for i := 0; i != batchSize; i++ {
			delta, ok := <-deltaCh
			if !ok {
				s.sendDeltas(ctx, deltas)
				return
			}
			deltas[i] = delta
		}
		s.sendDeltas(ctx, deltas)
	}
}

func (s *DeltaReceiverSvc) sendDeltas(ctx context.Context, deltas []model.Delta) {
	if !s.globalRepo.SendDeltas(ctx, deltas) {
		//run reconnects
		if s.localRepo.SaveDeltas(ctx, deltas) {
			return
		}
		// УСЁ ПРОПАЛО
		s.saveDeltasToFile(deltas)
	}
}

func (s *DeltaReceiverSvc) saveSnapshotToFile(snapshot []model.DepthSnapshotPart) {
	file, _ := os.Create("snapshot" + string(time.Now().UnixMilli()))
	data, _ := json.Marshal(snapshot)
	file.Write(data)
	file.Close()
}

func (s *DeltaReceiverSvc) saveDeltasToFile(deltas []model.Delta) {
	file, _ := os.Create("deltas" + string(time.Now().UnixMilli()))
	data, _ := json.Marshal(deltas)
	file.Write(data)
	file.Close()
}

func (s *DeltaReceiverSvc) Shutdown(ctx context.Context) {
	for _, recv := range s.deltaReceivers {
		recv.Shutdown(ctx)
	}
}

var (
	StopRetryErr = errors.New("")
)

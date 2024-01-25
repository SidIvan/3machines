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
	//GetFullSnapshot() (*model.DepthSnapshot, error)
}

type LocalRepo interface {
	SaveDeltas([]model.Delta) error
}

type GlobalRepo interface {
	SendDeltas([]model.Delta) error
}

type DeltaReceiver interface {
	ReceiveDeltas(ctx context.Context, deltaCh chan<- *model.Delta)
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

func (s *DeltaReceiverSvc) ReceiveDeltasPairs(ctx context.Context) {
	for _, deltaReceiver := range s.deltaReceivers {
		go func(ctx context.Context, deltaReceiver DeltaReceiver) {
			for {
				s.ReceivePair(ctx, deltaReceiver)
			}
		}(ctx, deltaReceiver)
	}
}

func (s *DeltaReceiverSvc) ReceivePair(ctx context.Context, deltaReceiver DeltaReceiver) {
	deltaCh := make(chan *model.Delta)
	go deltaReceiver.ReceiveDeltas(ctx, deltaCh)
	batchSize := s.cfg.GDBBatchSize
	for {
		deltas := make([]model.Delta, batchSize)
		for i := 0; i != batchSize; i++ {
			delta, ok := <-deltaCh
			if !ok {
				s.sendDeltas(deltas)
				return
			}
			deltas[i] = *delta
		}
		s.sendDeltas(deltas)
	}
}

func (s *DeltaReceiverSvc) sendDeltas(deltas []model.Delta) {
	err := s.globalRepo.SendDeltas(deltas)
	if err != nil {
		//run reconnects
		s.log.Error(err.Error())
		err = s.localRepo.SaveDeltas(deltas)
		if err != nil {
			//run reconnects
			s.log.Error(err.Error())
		}
		// УСЁ ПРОПАЛО
		s.saveToFile(deltas)
	}
}

func (s *DeltaReceiverSvc) saveToFile(deltas []model.Delta) {
	file, _ := os.Create(string(time.Now().UnixMilli()))
	data, _ := json.Marshal(deltas)
	file.Write(data)
	file.Close()
}

var (
	StopRetryErr = errors.New("")
)

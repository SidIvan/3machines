package svc

import (
	"DeltaReceiver/internal/common/model"
	csvc "DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/binance"
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

const BatchSize = 7500

type DeltaReceiver struct {
	logger               *zap.Logger
	receiver             *binance.DeltaReceiveClient
	localRepo            LocalRepo
	deltaStorage         csvc.DeltaStorage
	metrics              MetricsHolder
	symbols              []string
	shutdown             *atomic.Bool
	done                 chan struct{}
	deltaUpdateIdWatcher *cache.DeltaUpdateIdWatcher
	deltaHolesStorage    DeltaHolesStorage
}

func NewDeltaReceiver(
	cfg *binance.BinanceHttpClientConfig,
	symbols []string, localRepo LocalRepo,
	deltaStorage csvc.DeltaStorage,
	metrics MetricsHolder,
	deltaUpdateIdWatcher *cache.DeltaUpdateIdWatcher,
	deltaHolesStorage DeltaHolesStorage) *DeltaReceiver {
	if len(symbols) == 0 {
		return nil
	}
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &DeltaReceiver{
		logger:               log.GetLogger("DeltaReceiver"),
		receiver:             binance.NewDeltaReceiveClient(cfg, symbols),
		symbols:              symbols,
		localRepo:            localRepo,
		deltaStorage:         deltaStorage,
		metrics:              metrics,
		shutdown:             &shutdown,
		done:                 make(chan struct{}),
		deltaUpdateIdWatcher: deltaUpdateIdWatcher,
		deltaHolesStorage:    deltaHolesStorage,
	}
}

func (s *DeltaReceiver) StartReceiveDeltas(ctx context.Context) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.receiver.Connect(ctxWithTimeout); err != nil {
		return fmt.Errorf("%w", err)
	}
	go s.ReceiveAndSend(ctx)
	return nil
}

func (s *DeltaReceiver) ReceiveAndSend(ctx context.Context) {
	s.logger.Info(fmt.Sprintf("delta receiver with %d symbols started", len(s.symbols)))
	for !s.shutdown.Load() {
		batch, err := s.ReceiveBatch(ctx)
		if err != nil {
			s.logger.Error(err.Error())
		} else if batch != nil {
			s.metrics.ProcessDeltaMetrics(batch, Receive)
			if err = s.SendBatch(ctx, batch); err != nil {
				s.logger.Error(err.Error())
			}
			s.validateBatch(ctx, batch)
		}
	}
	s.done <- struct{}{}
}

func (s *DeltaReceiver) ReceiveBatch(ctx context.Context) ([]model.Delta, error) {
	var deltas []model.Delta
	for !s.shutdown.Load() && len(deltas) <= BatchSize {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		deltaMsg, err := s.receiver.ReceiveDeltaMessage(ctxWithTimeout)
		cancel()
		if err != nil {
			if s.shutdown.Load() {
				return nil, nil
			}
			return nil, fmt.Errorf("%w", err)
		}
		if deltaMsg != nil {
			for _, bid := range deltaMsg.Bids {
				deltas = append(deltas, model.NewDelta(deltaMsg.EventTime, bid[0], bid[1], deltaMsg.UpdateId, deltaMsg.FirstUpdateId, true, deltaMsg.Symbol))
			}
			for _, ask := range deltaMsg.Asks {
				deltas = append(deltas, model.NewDelta(deltaMsg.EventTime, ask[0], ask[1], deltaMsg.UpdateId, deltaMsg.FirstUpdateId, false, deltaMsg.Symbol))
			}
		}
	}
	return deltas, nil
}

func (s *DeltaReceiver) SendBatch(ctx context.Context, deltas []model.Delta) error {
	for i := 0; i < 3; i++ {
		if err := s.deltaStorage.SendDeltas(ctx, deltas); err == nil {
			s.metrics.ProcessDeltaMetrics(deltas, Send)
			return nil
		} else {
			s.logger.Error(err.Error())
			s.logger.Warn("failed send to Ch, retry")
		}
	}
	s.deltaStorage.Reconnect(ctx)
	s.logger.Warn("failed send to Ch, try save to mongo")
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveDeltas(ctx, deltas); err == nil {
			s.metrics.ProcessDeltaMetrics(deltas, Save)
			s.logger.Info("successfully saved to mongo")
			return nil
		} else {
			s.logger.Warn("failed save to mongo, retry")
			s.logger.Error(err.Error())
		}
	}
	s.logger.Warn("failed save to mongo, attempting save to file")
	// УСЁ ПРОПАЛО
	return s.saveDeltasToFile(deltas)
}

func (s *DeltaReceiver) saveDeltasToFile(deltas []model.Delta) error {
	file, err := os.Create("deltas" + strconv.FormatInt(time.Now().UnixMilli(), 10))
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	data, _ := json.Marshal(deltas)
	if _, err = file.Write(data); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	file.Close()
	return nil
}

func (s *DeltaReceiver) validateBatch(ctx context.Context, batch []model.Delta) {
	holes := s.deltaUpdateIdWatcher.GetHolesAndUpdate(batch)
	for _, hole := range holes {
		for i := 0; i < 3; i++ {
			if s.saveHole(ctx, hole) {
				break
			}
		}
	}
}

func (s *DeltaReceiver) saveHole(ctx context.Context, hole model.DeltaHole) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.deltaHolesStorage.SaveDeltaHole(ctx, hole); err != nil {
		s.logger.Error(err.Error())
		return false
	}
	return true
}

func (s *DeltaReceiver) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.receiver.Shutdown(ctx)
	<-s.done
	s.logger.Debug("successfully shutdown")
}

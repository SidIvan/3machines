package svc

import (
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/binance"
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

const BatchSize = 10000

type DeltaReceiver struct {
	logger     *zap.Logger
	receiver   *binance.DeltaReceiveClient
	localRepo  LocalRepo
	globalRepo GlobalRepo
	symbols    []string
	shutdown   *atomic.Bool
	done       chan struct{}
}

func NewDeltaReceiver(cfg *binance.BinanceHttpClientConfig, symbols []string, localRepo LocalRepo, globalRepo GlobalRepo) *DeltaReceiver {
	if len(symbols) == 0 {
		return nil
	}
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &DeltaReceiver{
		logger:     log.GetLogger("DeltaReceiver"),
		receiver:   binance.NewDeltaReceiveClient(cfg, symbols),
		symbols:    symbols,
		localRepo:  localRepo,
		globalRepo: globalRepo,
		shutdown:   &shutdown,
		done:       make(chan struct{}),
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
		} else {
			if err = s.SendBatch(ctx, batch); err != nil {
				s.logger.Error(err.Error())
			}
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
		if err := s.globalRepo.SendDeltas(ctx, deltas); err == nil {
			return nil
		} else {
			s.logger.Error(err.Error())
			s.logger.Warn("failed send to Ch, retry")
		}
	}
	s.globalRepo.Reconnect(ctx)
	s.logger.Warn("failed send to Ch, try save to mongo send timestamp")
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveDeltas(ctx, deltas); err == nil {
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

func (s *DeltaReceiver) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.receiver.Shutdown(ctx)
	<-s.done
	s.logger.Debug("successfully shutdown")
}

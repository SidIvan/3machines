package svc

import (
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
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

type TickerReceiver struct {
	logger     *zap.Logger
	receiver   *binance.BookTickerClient
	localRepo  LocalRepo
	globalRepo GlobalRepo
	symbols    []string
	shutdown   *atomic.Bool
	done       chan struct{}
}

func NewTickerReceiver(cfg *binance.BinanceHttpClientConfig, symbols []string, localRepo LocalRepo, globalRepo GlobalRepo) *TickerReceiver {
	if len(symbols) == 0 {
		return nil
	}
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &TickerReceiver{
		logger:     log.GetLogger("TickerReceiver"),
		receiver:   binance.NewBookTickerClient(cfg, symbols),
		symbols:    symbols,
		localRepo:  localRepo,
		globalRepo: globalRepo,
		shutdown:   &shutdown,
		done:       make(chan struct{}),
	}
}

func (s *TickerReceiver) StartReceiveTicks(ctx context.Context) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.receiver.Connect(ctxWithTimeout); err != nil {
		return fmt.Errorf("%w", err)
	}
	go s.ReceiveAndSend(ctx)
	return nil
}

func (s *TickerReceiver) ReceiveAndSend(ctx context.Context) {
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

func (s *TickerReceiver) ReceiveBatch(ctx context.Context) ([]bmodel.SymbolTick, error) {
	var ticks []bmodel.SymbolTick
	for !s.shutdown.Load() && len(ticks) <= BatchSize {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		tick, err := s.receiver.ReceiveTicks(ctxWithTimeout)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		ticks = append(ticks, *tick)
	}
	return ticks, nil
}

func (s *TickerReceiver) SendBatch(ctx context.Context, ticks []bmodel.SymbolTick) error {
	curTime := time.Now().UnixMilli()
	s.logger.Info(fmt.Sprintf("sending batch of %d ticks, send timestamp %d", len(ticks), curTime))
	for i := 0; i < 3; i++ {
		if err := s.globalRepo.SendBookTicks(ctx, ticks); err == nil {
			s.logger.Info(fmt.Sprintf("successfully sent to Ch, send timestamp %d", curTime))
			return nil
		} else {
			s.logger.Error(err.Error())
			s.logger.Warn(fmt.Sprintf("failed send to Ch, retry, timestamp %d", curTime))
		}
	}
	s.globalRepo.Reconnect(ctx)
	s.logger.Warn(fmt.Sprintf("failed send to Ch, try save to mongo send timestamp %d", curTime))
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveBookTicker(ctx, ticks); err == nil {
			s.logger.Info(fmt.Sprintf("successfully saved to mongo, send timestamp %d", curTime))
			return nil
		} else {
			s.logger.Warn(fmt.Sprintf("failed save to mongo, retry, timestamp %d", curTime))
			s.logger.Error(err.Error())
		}
	}
	s.logger.Warn(fmt.Sprintf("failed save to mongo, attempting save to file, send timestamp %d", curTime))
	// УСЁ ПРОПАЛО
	return s.saveTicksToFile(ticks)
}

func (s *TickerReceiver) saveTicksToFile(deltas []bmodel.SymbolTick) error {
	file, err := os.Create("ticks" + strconv.FormatInt(time.Now().UnixMilli(), 10))
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

func (s *TickerReceiver) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.receiver.Shutdown(ctx)
	s.logger.Debug("before writing to chan")
	<-s.done
	s.logger.Debug("successfully shutdown")
}

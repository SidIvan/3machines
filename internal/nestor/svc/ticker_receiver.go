package svc

import (
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
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

type TickerReceiver struct {
	logger           *zap.Logger
	receiver         *binance.BookTickerClient
	localRepo        LocalRepo
	bookTicksStorage BookTicksStorage
	metrics          MetricsHolder
	symbols          []string
	shutdown         *atomic.Bool
	done             chan struct{}
	useLocalStorage  bool
}

func NewTickerReceiver(cfg *binance.BinanceHttpClientConfig, symbols []string, localRepo LocalRepo, bookTicksStorage BookTicksStorage, metrics MetricsHolder, useLocalStorage bool) *TickerReceiver {
	if len(symbols) == 0 {
		return nil
	}
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &TickerReceiver{
		logger:           log.GetLogger("TickerReceiver"),
		receiver:         binance.NewBookTickerClient(cfg, symbols),
		symbols:          symbols,
		localRepo:        localRepo,
		bookTicksStorage: bookTicksStorage,
		metrics:          metrics,
		shutdown:         &shutdown,
		done:             make(chan struct{}),
		useLocalStorage:  useLocalStorage,
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
	s.logger.Info(fmt.Sprintf("ticks receiver with %d symbols started", len(s.symbols)))
	for !s.shutdown.Load() {
		batch, err := s.ReceiveBatch(ctx)
		if err != nil {
			s.metrics.IncTicksRecvErr()
			s.logger.Error(err.Error())
		} else {
			s.metrics.ProcessTickMetrics(batch, Receive)
			go func() {
				if err = s.sendBatch(ctx, batch); err != nil {
					s.logger.Error(err.Error())
				}
			}()
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
			return ticks, fmt.Errorf("%w", err)
		}
		if tick != nil {
			ticks = append(ticks, *tick)
		}
	}
	return ticks, nil
}

func (s *TickerReceiver) sendBatch(ctx context.Context, ticks []bmodel.SymbolTick) error {
	err := s.sendBatchToGlobalRepo(ctx, ticks)
	if err == nil {
		return nil
	}
	err = s.sendBatchToLocalRepo(ctx, ticks)
	if err == nil {
		return nil
	}
	// УСЁ ПРОПАЛО
	return s.saveTicksToFile(ticks)
}

func (s *TickerReceiver) sendBatchToGlobalRepo(ctx context.Context, ticks []bmodel.SymbolTick) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = s.bookTicksStorage.SendBookTicks(ctx, ticks); err == nil {
			s.metrics.ProcessTickMetrics(ticks, Send)
			return nil
		} else {
			s.logger.Error(err.Error())
		}
	}
	return err
}

func (s *TickerReceiver) sendBatchToLocalRepo(ctx context.Context, ticks []bmodel.SymbolTick) error {
	if !s.useLocalStorage {
		s.logger.Debug("cannot use local storage")
		return nil
	}
	var err error
	for i := 0; i < 3; i++ {
		if err := s.localRepo.SaveBookTicker(ctx, ticks); err == nil {
			s.metrics.ProcessTickMetrics(ticks, Save)
			return nil
		} else {
			s.logger.Error(err.Error())
		}
	}
	return err
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
	<-s.done
	s.logger.Debug("successfully shutdown")
}

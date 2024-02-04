package web

import (
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"sync/atomic"
	"time"
)

type DeltaReceiverWS struct {
	logger           *zap.Logger
	Receiver         *binance.DeltaReceiveClient
	symbol           bmodel.Symbol
	ReconnectPeriodM int16
	shutdown         *atomic.Bool
}

func NewDeltaReceiverWs(cfg *binance.BinanceHttpClientConfig, pair string, period, reconnectPeriodM int16) *DeltaReceiverWS {
	var shutdown atomic.Bool
	shutdown.Store(false)
	return &DeltaReceiverWS{
		logger:           log.GetLogger(fmt.Sprintf("BinanceDeltaReceiverWS: %s", pair)),
		Receiver:         binance.NewDeltaReceiveClient(cfg, pair, period),
		symbol:           bmodel.ParseSymbol(pair),
		ReconnectPeriodM: reconnectPeriodM,
		shutdown:         &shutdown,
	}
}

func (s DeltaReceiverWS) GetSymbol() model.Symbol {
	return convBinanceSymb2Symb(s.symbol)
}

func (s DeltaReceiverWS) Reconnect() {
	time.Sleep(time.Duration(s.ReconnectPeriodM) * time.Minute)
	if s.shutdown.Load() {
		return
	}
	s.logger.Info("reconnecting")
	if err := s.Receiver.Reconnect(); err != nil {
		s.logger.Error(err.Error())
	}
}

func (s DeltaReceiverWS) ReceiveDeltas(ctx context.Context) []model.Delta {
	deltaMsg := s.Receiver.ReceiveDelta(ctx)
	if deltaMsg == nil {
		s.logger.Info(fmt.Sprintf("delta receiver of pair %s ended", s.symbol))
		return nil
	}
	var deltas []model.Delta
	for _, bid := range deltaMsg.Bids {
		price, err := strconv.ParseFloat(bid[0], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		count, err := strconv.ParseFloat(bid[1], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		deltas = append(deltas, model.NewDelta(deltaMsg.EventTime, price, count, deltaMsg.UpdateId, deltaMsg.FirstUpdateId, true, convBinanceSymb2Symb(s.symbol)))
	}
	for _, ask := range deltaMsg.Asks {
		price, err := strconv.ParseFloat(ask[0], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		count, err := strconv.ParseFloat(ask[1], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		deltas = append(deltas, model.NewDelta(deltaMsg.EventTime, price, count, deltaMsg.UpdateId, deltaMsg.FirstUpdateId, false, convBinanceSymb2Symb(s.symbol)))
	}
	return deltas
}

func (s DeltaReceiverWS) Shutdown(ctx context.Context) {
	s.shutdown.Store(true)
	s.Receiver.Shutdown(ctx)
}

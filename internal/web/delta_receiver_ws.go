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
	"time"
)

type DeltaReceiverWS struct {
	logger           *zap.Logger
	Receiver         *binance.DeltaReceiveClient
	symbol           bmodel.Symbol
	reconnectPeriodM int16
}

func NewDeltaReceiverWs(cfg *binance.BinanceHttpClientConfig, pair string, period, reconnectPeriodM int16) *DeltaReceiverWS {
	return &DeltaReceiverWS{
		logger:           log.GetLogger(fmt.Sprintf("BinanceDeltaReceiverWS: %s", pair)),
		Receiver:         binance.NewDeltaReceiveClient(cfg, pair, period),
		symbol:           bmodel.ParseSymbol(pair),
		reconnectPeriodM: reconnectPeriodM,
	}
}

func (s DeltaReceiverWS) GetSymbol() model.Symbol {
	return convBinanceSymb2Symb(s.symbol)
}

func (s DeltaReceiverWS) ReceiveDeltas(ctx context.Context, ch chan<- model.Delta) {
	defer close(ch)
	deltaMessageCh := make(chan *bmodel.DeltaMessage)
	go func() {
		for {
			time.Sleep(time.Duration(s.reconnectPeriodM) * time.Minute)
			if err := s.Receiver.Reconnect(); err != nil {
				s.logger.Error(err.Error())
			}
		}
	}()
	go func() {
		if err := s.Receiver.ReceiveDeltas(deltaMessageCh); err != nil {
			s.logger.Error(err.Error())
		}
	}()
	for {
		deltaMsg, ok := <-deltaMessageCh
		if !ok {
			return
		}
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
			ch <- model.NewDelta(deltaMsg.EventTime, price, count, deltaMsg.UpdateId, true, convBinanceSymb2Symb(s.symbol))
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
			ch <- model.NewDelta(deltaMsg.EventTime, price, count, deltaMsg.UpdateId, false, convBinanceSymb2Symb(s.symbol))
		}
	}
}

func (s DeltaReceiverWS) Shutdown(ctx context.Context) {
	s.Receiver.Shutdown(ctx)
}

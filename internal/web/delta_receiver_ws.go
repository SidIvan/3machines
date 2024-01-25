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
)

type DeltaReceiverWS struct {
	logger   *zap.Logger
	Receiver *binance.DeltaReceiveClient
}

func NewDeltaReceiverWs(cfg *binance.BinanceHttpClientConfig, pair string, period int16) *DeltaReceiverWS {
	return &DeltaReceiverWS{
		logger:   log.GetLogger(fmt.Sprintf("BinanceDeltaReceiverWS: %s", pair)),
		Receiver: binance.NewDeltaReceiveClient(cfg, pair, period),
	}
}

func (s DeltaReceiverWS) ReceiveDeltas(ctx context.Context, ch chan<- *model.Delta) {
	deltaMessageCh := make(chan *bmodel.DeltaMessage)
	go s.Receiver.ReceiveDeltas(deltaMessageCh)
	for {
		deltaMsg := <-deltaMessageCh
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
			ch <- &model.Delta{
				Timestamp: deltaMsg.EventTime,
				Price:     price,
				Count:     count,
				T:         true,
			}
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
			ch <- &model.Delta{
				Timestamp: deltaMsg.EventTime,
				Price:     price,
				Count:     count,
				T:         false,
			}
		}
	}
}

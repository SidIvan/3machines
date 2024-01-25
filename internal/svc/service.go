package svc

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
)

type BinanceClient interface {
	GetFullSnapshot() (*model.DepthSnapshot, error)
}

type LocalRepo interface {
}

type GlobalRepo interface {
}

type DeltaReceiver interface {
}

type MetricsHolder interface {
}

type DeltaReceiverSvc struct {
	log           *zap.Logger
	binanceClient BinanceClient
	deltaReceiver DeltaReceiver
	localRepo     LocalRepo
	globalRepo    GlobalRepo
	metricsHolder MetricsHolder
	cfg           *conf.AppConfig
}

func NewDeltaReceiverSvc(config *conf.AppConfig, binanceClient BinanceClient, deltaReceiver DeltaReceiver, localRepo LocalRepo,
	globalRepo GlobalRepo, metricsHolder MetricsHolder) *DeltaReceiverSvc {
	return &DeltaReceiverSvc{
		log:           log.GetLogger("DeltaReceiverSvc"),
		binanceClient: binanceClient,
		deltaReceiver: deltaReceiver,
		localRepo:     localRepo,
		globalRepo:    globalRepo,
		metricsHolder: metricsHolder,
		cfg:           config,
	}
}

// "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
func (s *DeltaReceiverSvc) ReceiveDeltas(ctx context.Context) {
	for pair, period := range s.cfg.BinanceHttpConfig.Pair2Period {
		go s.ReceivePair(ctx, pair, period)
	}
}

func (s *DeltaReceiverSvc) ReceivePair(ctx context.Context, pair string, period int16) {
	url := fmt.Sprintf("%s%s@depth@%dms", s.cfg.BinanceHttpConfig.DeltaStreamBaseUriConfig.GetBaseUri(), pair, period)
	for {
		conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
		if resp.StatusCode == http.StatusTeapot || resp.StatusCode == http.StatusTooManyRequests {

		}
		if err != nil {
			s.log.Error("dial: " + err.Error())
		}
		conn.Close()
	}
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("read: ", err)
				return
			}
			stringDeltas = append(stringDeltas, string(message))
			fmt.Println(string(message))
			//fmt.Println(string(message))
			var delta Delta
			if err := json.Unmarshal(message, &delta); err != nil {
				fmt.Println("Error unmarshalling JSON:", err)
				return
			}
			deltasMutex.Lock()
			deltas = append(deltas, delta)
			deltasMutex.Unlock()
			go SaveDelta(context.Background(), &delta)
		}
	}()
	<-ctx.Done()
}

var (
	StopRetryErr = errors.New("")
)

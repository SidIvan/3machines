package binance

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type DeltaReceiveClient struct {
	logger       *zap.Logger
	baseUri      string
	pair         string
	period       int16
	receiveTimeS int
}

func NewDeltaReceiveClient(cfg *BinanceHttpClientConfig, pair string, period int16) *DeltaReceiveClient {
	return &DeltaReceiveClient{
		logger:       log.GetLogger("DeltaReceiveClient"),
		baseUri:      cfg.DeltaStreamBaseUriConfig.GetBaseUri(),
		period:       period,
		pair:         pair,
		receiveTimeS: cfg.receiveTimeS,
	}
}

func (s *DeltaReceiveClient) ReceiveDeltas(ch chan *model.DeltaMessage) error {
	defer close(ch)
	if isBanned() {
		return RequestRejectedErr
	}
	dialer, resp, err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s/ws%s@depth%dms", s.baseUri, s.pair, s.period), nil)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	if resp.StatusCode == http.StatusTeapot {
		return banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return banBinanceRequests(resp, WeightLimitExceededErr)
	}
	end := time.Now().Add(time.Duration(s.receiveTimeS) * time.Second)
	for time.Now().Before(end) {
		_, msg, err := dialer.ReadMessage()
		if err != nil {
			s.logger.Error(err.Error())
			return err
		}
		var deltaMsg model.DeltaMessage
		err = json.Unmarshal(msg, &deltaMsg)
		if err != nil {
			s.logger.Error(err.Error())
			return err
		}
		ch <- &deltaMsg
	}
	return nil
}

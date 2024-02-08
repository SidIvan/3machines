package binance

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
)

type ReconMutex struct {
	lastUpdId *atomic.Int64
	ch        chan struct{}
}

type DeltaReceiveClient struct {
	logger     *zap.Logger
	baseUri    string
	pair       string
	period     int16
	shutdown   *atomic.Bool
	dialer     *websocket.Conn
	dialerMut  *sync.Mutex
	reconMutex *ReconMutex
}

func (s *DeltaReceiveClient) setDialer(dialer *websocket.Conn) {
	_, msg, err := dialer.ReadMessage()
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	var deltaMsg model.DeltaMessage
	err = json.Unmarshal(msg, &deltaMsg)
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	s.reconMutex.lastUpdId.Store(deltaMsg.UpdateId)
	<-s.reconMutex.ch
	s.dialerMut.Lock()
	defer s.dialerMut.Unlock()
	if s.dialer != nil {
		err := s.dialer.Close()
		if err != nil {
			return
		}
	}
	s.dialer = dialer
}

func NewDeltaReceiveClient(cfg *BinanceHttpClientConfig, pair string, period int16) *DeltaReceiveClient {
	var mut sync.Mutex
	var shutdown atomic.Bool
	var lastUpdId atomic.Int64
	lastUpdId.Store(math.MaxInt64)
	ch := make(chan struct{})
	shutdown.Store(false)
	client := DeltaReceiveClient{
		logger:    log.GetLogger("DeltaReceiveClient [" + pair + "]"),
		baseUri:   cfg.DeltaStreamBaseUriConfig.GetBaseUri(),
		period:    period,
		pair:      pair,
		dialerMut: &mut,
		shutdown:  &shutdown,
		reconMutex: &ReconMutex{
			lastUpdId: &lastUpdId,
			ch:        ch,
		},
	}
	return &client
}

func (s *DeltaReceiveClient) Reconnect() error {
	s.logger.Debug("start of reconnecting")
	if s.shutdown.Load() {
		s.logger.Warn("graceful shutdown processing")
		return nil
	}
	if dialer, err := s.getDialer(); dialer != nil {
		if s.shutdown.Load() {
			s.logger.Warn("graceful shutdown processing")
			return nil
		}
		s.setDialer(dialer)
	} else {
		return err
	}
	s.logger.Debug("successfully reconnected")
	return nil
}

func (s *DeltaReceiveClient) getDialer() (*websocket.Conn, error) {
	d := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment,
	}
	dialer, resp, err := d.Dial(fmt.Sprintf("%sws/%s@depth@%dms", s.baseUri, s.pair, s.period), nil)
	if resp.StatusCode == http.StatusTeapot {
		return nil, banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, banBinanceRequests(resp, WeightLimitExceededErr)
	}
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return dialer, nil
}

func (s *DeltaReceiveClient) ReceiveDeltaMessage(ctx context.Context) *model.DeltaMessage {
	if isBanned() || s.shutdown.Load() {
		return nil
	}
	s.dialerMut.Lock()
	if s.dialer == nil {
		dialer, _ := s.getDialer()
		s.dialer = dialer
	}
	_, msg, err := s.dialer.ReadMessage()
	s.dialerMut.Unlock()
	if err != nil {
		if s.shutdown.Load() {
			return nil
		}
		s.logger.Error(err.Error())
		return nil
	}
	var deltaMsg model.DeltaMessage
	err = json.Unmarshal(msg, &deltaMsg)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	if s.reconMutex.lastUpdId.Load() <= deltaMsg.FirstUpdateId {
		s.reconMutex.lastUpdId.Store(math.MaxInt64)
		s.reconMutex.ch <- struct{}{}
	}
	return &deltaMsg
}

func (s *DeltaReceiveClient) Shutdown(ctx context.Context) {
	if !s.shutdown.Load() {
		s.shutdown.Store(true)
		if s.dialer != nil {
			err := s.dialer.Close()
			if err != nil {
				s.logger.Error(err.Error())
			}
		}
	}
}

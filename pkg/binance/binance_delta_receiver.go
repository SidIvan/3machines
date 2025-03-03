package binance

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type DeltaReceiveClient struct {
	logger   *zap.Logger
	baseUri  string
	symbols  []string
	shutdown *atomic.Bool
	dialer   *websocket.Conn
}

func NewDeltaReceiveClient(cfg *BinanceHttpClientConfig, symbols []string) *DeltaReceiveClient {
	var shutdown atomic.Bool
	shutdown.Store(false)
	client := DeltaReceiveClient{
		logger:   log.GetLogger("DeltaReceiveClient"),
		baseUri:  cfg.StreamBaseUriConfig.GetBaseUri(),
		symbols:  symbols,
		shutdown: &shutdown,
	}
	return &client
}

func (s *DeltaReceiveClient) formWSUri() string {
	return fmt.Sprintf("%s/ws/%s@depth@100ms", s.baseUri, strings.Join(s.symbols, "@depth@100ms/"))
}

func (s *DeltaReceiveClient) ConnectWs(ctx context.Context) error {
	d := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment,
	}
	dialer, resp, err := d.Dial(s.formWSUri(), nil)
	if resp.StatusCode == http.StatusTeapot {
		return banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return banBinanceRequests(resp, WeightLimitExceededErr)
	}
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}

	s.dialer = dialer
	return nil
}

func (s *DeltaReceiveClient) Reconnect(ctx context.Context) error {
	s.logger.Debug("start of reconnecting")
	if s.shutdown.Load() {
		s.logger.Warn("graceful shutdown processing")
		return nil
	}
	if err := s.dialer.Close(); err != nil {
		s.logger.Warn(fmt.Errorf("connection was not closed %w", err).Error())
	}
	if err := s.ConnectWs(ctx); err != nil {
		s.logger.Warn(fmt.Errorf("connection was not reset %w", err).Error())
		return err
	}
	return nil
}

func (s *DeltaReceiveClient) Recv(ctx context.Context) (model.DeltaMessage, error) {
	if isBanned() || s.shutdown.Load() {
		return model.DeltaMessage{}, nil
	}
	if s.dialer == nil {
		if err := s.ConnectWs(ctx); err != nil {
			return model.DeltaMessage{}, err
		}
	}
	for i := 0; ; i++ {
		_, msg, err := s.dialer.ReadMessage()
		if err == nil {
			var deltaMsg model.DeltaMessage
			err = json.Unmarshal(msg, &deltaMsg)
			if err != nil {
				s.logger.Error(err.Error())
				return model.DeltaMessage{}, fmt.Errorf("error while unmarshaling delta message %w", err)
			}
			return deltaMsg, nil
		}
		if s.shutdown.Load() {
			return model.DeltaMessage{}, nil
		}
		s.logger.Error(s.formWSUri())
		s.logger.Warn(fmt.Errorf("error while getting delta message, reconnect %w", err).Error())
		if err = s.Reconnect(ctx); err != nil && i == 3 {
			return model.DeltaMessage{}, err
		}
	}
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

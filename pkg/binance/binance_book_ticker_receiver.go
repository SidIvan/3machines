package binance

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

type BookTickerClient struct {
	logger    *zap.Logger
	baseUri   string
	symbols   []string
	shutdown  *atomic.Bool
	dialer    *websocket.Conn
	dialerMut *sync.Mutex
}

func NewBookTickerClient(cfg *BinanceHttpClientConfig, symbols []string) *BookTickerClient {
	var mut sync.Mutex
	var shutdown atomic.Bool
	shutdown.Store(false)
	client := BookTickerClient{
		logger:    log.GetLogger("DeltaReceiveClient"),
		baseUri:   cfg.DeltaStreamBaseUriConfig.GetBaseUri(),
		symbols:   symbols,
		dialerMut: &mut,
		shutdown:  &shutdown,
	}
	return &client
}

func (s *BookTickerClient) formWSUri() string {
	return fmt.Sprintf("%sws/%s@bookTicker", s.baseUri, strings.Join(s.symbols, "@bookTicker/"))
}

func (s *BookTickerClient) Connect(ctx context.Context) error {
	s.dialerMut.Lock()
	defer s.dialerMut.Unlock()
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

func (s *BookTickerClient) Reconnect(ctx context.Context) error {
	s.logger.Debug("start of reconnecting")
	if s.shutdown.Load() {
		s.logger.Warn("graceful shutdown processing")
		return nil
	}
	for i := 0; i < 3; i++ {
		if err := s.dialer.Close(); err != nil {
			s.logger.Warn(fmt.Errorf("connection was not closed %w", err).Error())
		} else {
			break
		}
	}
	if err := s.Connect(ctx); err != nil {
		s.logger.Warn(fmt.Errorf("connection was not reset %w", err).Error())
		return err
	}
	return nil
}

func (s *BookTickerClient) ReceiveTicks(ctx context.Context) (*model.SymbolTick, error) {
	if isBanned() || s.shutdown.Load() {
		return nil, nil
	}
	if s.dialer == nil {
		if err := s.Connect(ctx); err != nil {
			return nil, err
		}
	}
	for i := 0; ; i++ {
		s.dialerMut.Lock()
		_, msg, err := s.dialer.ReadMessage()
		s.dialerMut.Unlock()
		if err == nil {
			var tick model.SymbolTick
			err = json.Unmarshal(msg, &tick)
			if err != nil {
				s.logger.Error(err.Error())
				return nil, fmt.Errorf("error while unmarshaling delta message %w", err)
			}
			return &tick, nil
		}
		if s.shutdown.Load() {
			return nil, nil
		}
		s.logger.Warn("error while getting delta message, reconnect")
		if err = s.Reconnect(ctx); err != nil && i == 3 {
			return nil, err
		}
	}
}

func (s *BookTickerClient) Shutdown(ctx context.Context) {
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

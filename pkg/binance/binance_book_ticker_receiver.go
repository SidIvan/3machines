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
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type BookTickerClient struct {
	logger              *zap.Logger
	wsBaseUri           string
	useAllTickersStream bool
	symbols             []string
	shutdown            *atomic.Bool
	dialer              *websocket.Conn
}

func NewBookTickerClient(cfg *BinanceHttpClientConfig, symbols []string) *BookTickerClient {
	var shutdown atomic.Bool
	shutdown.Store(false)
	client := BookTickerClient{
		logger:              log.GetSTDOutLogger("DeltaReceiveClient"),
		wsBaseUri:           cfg.StreamBaseUriConfig.GetBaseUri() + "/ws",
		useAllTickersStream: cfg.UseAllTickersStream,
		symbols:             symbols,
		shutdown:            &shutdown,
	}
	return &client
}

func (s *BookTickerClient) formWSUri() string {
	if s.useAllTickersStream {
		return fmt.Sprintf("%s/!bookTicker", s.wsBaseUri)
	}
	return fmt.Sprintf("%s/%s@bookTicker", s.wsBaseUri, strings.Join(s.symbols, "@bookTicker/"))
}

func (s *BookTickerClient) ConnectWs(ctx context.Context) error {
	d := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment,
		ReadBufferSize:  10240,
		WriteBufferSize: 10240,
	}
	dialUri := s.formWSUri()
	s.logger.Debug("start dial with uri " + dialUri)
	dialer, resp, err := d.Dial(s.formWSUri(), nil)
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
	s.dialer = dialer
	return nil
}

func (s *BookTickerClient) Reconnect(ctx context.Context) error {
	s.logger.Debug("start of reconnecting")
	if s.shutdown.Load() {
		s.logger.Warn("graceful shutdown processing")
		return nil
	}
	s.dialer.Close()
	if err := s.ConnectWs(ctx); err != nil {
		s.logger.Warn(fmt.Errorf("connection was not reset %w", err).Error())
		return err
	}
	return nil
}

func (s *BookTickerClient) Recv(ctx context.Context) (model.SymbolTick, error) {
	if isBanned() || s.shutdown.Load() {
		return model.SymbolTick{}, nil
	}
	if s.dialer == nil {
		if err := s.ConnectWs(ctx); err != nil {
			return model.SymbolTick{}, err
		}
	}
	for i := 0; ; i++ {
		_, msg, err := s.dialer.ReadMessage()
		if err == nil {
			var tick model.SymbolTick
			err = json.Unmarshal(msg, &tick)
			if err != nil {
				s.logger.Error(err.Error())
				return model.SymbolTick{}, fmt.Errorf("error while unmarshaling tick message %w", err)
			}
			tick.Timestamp = time.Now().UnixMilli()
			return tick, nil
		}
		if s.shutdown.Load() {
			return model.SymbolTick{}, nil
		}
		s.logger.Warn(err.Error())
		if s.shutdown.Load() {
			return model.SymbolTick{}, nil
		}
		s.logger.Warn("error while getting tick message, reconnect")
		if err = s.Reconnect(ctx); err != nil && i == 3 {
			return model.SymbolTick{}, err
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

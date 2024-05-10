package binance

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type BinanceHttpClient struct {
	logger  *zap.Logger
	client  *http.Client
	baseUri string
}

func NewBinanceHttpClient(cfg *BinanceHttpClientConfig) *BinanceHttpClient {
	return &BinanceHttpClient{
		logger:  log.GetLogger("BinanceHttpClient"),
		client:  &http.Client{},
		baseUri: cfg.HttpBaseUriConfig.GetBaseUri(),
	}
}

func (s BinanceHttpClient) GetBookTicker(ctx context.Context) ([]model.SymbolTick, error) {
	if isBanned() {
		return nil, RequestRejectedErr
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxWithTimeout, http.MethodGet, fmt.Sprintf("%sapi/v3/ticker/bookTicker", s.baseUri), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("error while getting book ticker %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error while getting book ticker %w", err)
	}
	if resp.StatusCode == http.StatusTeapot {
		return nil, banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, banBinanceRequests(resp, WeightLimitExceededErr)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	var bookTicks []model.SymbolTick
	err = json.Unmarshal(respBody, &bookTicks)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		s.logger.Warn(err.Error())
	}
	return bookTicks, nil
}

func (s BinanceHttpClient) GetFullExchangeInfo(ctx context.Context) (*model.ExchangeInfo, error) {
	if isBanned() {
		return nil, RequestRejectedErr
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%sapi/v3/exchangeInfo", s.baseUri), http.NoBody)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	if resp.StatusCode == http.StatusTeapot {
		return nil, banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, banBinanceRequests(resp, WeightLimitExceededErr)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	var exInfo model.ExchangeInfo
	err = json.Unmarshal(respBody, &exInfo)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		s.logger.Warn(err.Error())
	}
	return &exInfo, nil
}

func (s BinanceHttpClient) GetFullSnapshot(ctx context.Context, symbol string, depthLimit int, headerType string) (*model.DepthSnapshot, string, error) {
	if isBanned() {
		return nil, "", RequestRejectedErr
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%sapi/v3/depth?symbol=%s&limit=%d", s.baseUri, symbol, depthLimit), http.NoBody)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, "", err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, "", err
	}
	if resp.StatusCode == http.StatusTeapot {
		return nil, "", banBinanceRequests(resp, TeapotErr)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, "", banBinanceRequests(resp, WeightLimitExceededErr)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, "", err
	}
	err = resp.Body.Close()
	if err != nil {
		s.logger.Warn(err.Error())
	}
	var snapshot model.DepthSnapshot
	err = json.Unmarshal(respBody, &snapshot)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, "", err
	}
	return &snapshot, resp.Header.Get(fmt.Sprintf("X-Mbx-Used-Weight-%s", headerType)), nil
}

var (
	TeapotErr              = fmt.Errorf("got teapot http response status, current IP banned by binance")
	WeightLimitExceededErr = fmt.Errorf("too many requests, weight limit exceeded")
	InvalidBinanceDataErr  = fmt.Errorf("got invalid data from binance server")
)

package binance

import (
	"DeltaReceiver/internal/svc"
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

func (s BinanceHttpClient) GetFullSnapshot(ctx context.Context, symbol model.Symbol, depthLimit int) (*model.DepthSnapshot, error) {
	if isBanned() {
		return nil, RequestRejectedErr
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=%d", s.baseUri, symbol, depthLimit), http.NoBody)
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
	err = resp.Body.Close()
	if err != nil {
		s.logger.Warn(err.Error())
	}
	var snapshot model.DepthSnapshot
	err = json.Unmarshal(respBody, &snapshot)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return &snapshot, nil
}

var (
	TeapotErr              = fmt.Errorf("got teapot http response status, current IP banned by binance%w", svc.StopRetryErr)
	WeightLimitExceededErr = fmt.Errorf("too many requests, weight limit exceeded%w", svc.StopRetryErr)
	InvalidBinanceDataErr  = fmt.Errorf("got invalid data from binance server%w", svc.StopRetryErr)
)

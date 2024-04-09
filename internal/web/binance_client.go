package web

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type BinanceClient struct {
	logger      *zap.Logger
	client      *binance.BinanceHttpClient
	exInfoCache *cache.ExchangeInfoCache
}

func NewBinanceClient(cfg *binance.BinanceHttpClientConfig, exInfoCache *cache.ExchangeInfoCache) *BinanceClient {
	return &BinanceClient{
		logger:      log.GetLogger("BinanceClient"),
		client:      binance.NewBinanceHttpClient(cfg),
		exInfoCache: exInfoCache,
	}
}

func (s BinanceClient) GetFullSnapshot(ctx context.Context, symbol string, depth int) ([]model.DepthSnapshotPart, string, error) {
	s.logger.Info(fmt.Sprintf("get full shapshot [%s]", symbol))
	snapshot, curLimit, err := s.client.GetFullSnapshot(ctx, symbol, depth, s.exInfoCache.GetVal().GetSuffixOfLimitHeader())
	if err != nil {
		s.logger.Error(err.Error())
		return nil, curLimit, err
	}
	timestamp := time.Now().UnixMilli()
	var snapshotParts []model.DepthSnapshotPart
	for _, bid := range snapshot.Bids {
		snapshotParts = append(snapshotParts, model.NewDepthSnapshotPart(snapshot.LastUpdateId, true, bid[0], bid[1], symbol, timestamp))
	}
	for _, ask := range snapshot.Asks {
		snapshotParts = append(snapshotParts, model.NewDepthSnapshotPart(snapshot.LastUpdateId, false, ask[0], ask[1], symbol, timestamp))
	}
	return snapshotParts, curLimit, nil
}

func (s BinanceClient) GetFullExchangeInfo(ctx context.Context) (*bmodel.ExchangeInfo, error) {
	exInfo, err := s.client.GetFullExchangeInfo(ctx)
	fmt.Println(exInfo)
	fmt.Println(err.Error())
	if err != nil {
		s.exInfoCache.SetVal(exInfo)
	}
	return exInfo, err
}

func (s BinanceClient) GetBookTicks(ctx context.Context) ([]bmodel.SymbolTick, error) {
	s.logger.Info("get full book ticker")
	ticks, err := s.client.GetBookTicker(ctx)
	if err == nil {
		s.logger.Info("successfully got book ticker")
	}
	return ticks, err
}

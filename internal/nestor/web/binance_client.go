package web

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type BinanceClient struct {
	logger      *zap.Logger
	dataType    bmodel.DataType
	client      *binance.BinanceHttpClient
	exInfoCache *cache.ExchangeInfoCache
}

func NewBinanceClient(dataType bmodel.DataType, cfg *binance.BinanceHttpClientConfig, exInfoCache *cache.ExchangeInfoCache) *BinanceClient {
	return &BinanceClient{
		logger:      log.GetLogger(fmt.Sprintf("BinanceClient[%s]", dataType)),
		client:      binance.NewBinanceHttpClient(dataType, cfg),
		exInfoCache: exInfoCache,
	}
}

func (s BinanceClient) GetFullSnapshot(ctx context.Context, symbol string, depth int) ([]model.DepthSnapshotPart, string, error) {
	s.logger.Info(fmt.Sprintf("get full shapshot [%s]", symbol))
	snapshot, curLimit, err := s.client.GetFullSnapshot(ctx, symbol, depth, s.exInfoCache.GetSuffixOfLimitHeader())
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

func (s BinanceClient) GetFullExchangeInfo(ctx context.Context, dataType bmodel.DataType) (bmodel.ExInfo, error) {
	var exInfo bmodel.ExInfo
	var err error
	if dataType == bmodel.FuturesCoin {
		exInfo, err = s.client.GetCoinFullExchangeInfo(ctx)
	} else {
		exInfo, err = s.client.GetFullExchangeInfo(ctx)
	}
	if err == nil {
		s.exInfoCache.SetVal(exInfo)
		return exInfo, nil
	}
	return exInfo, err
}

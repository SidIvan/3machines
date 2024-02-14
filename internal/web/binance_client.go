package web

import (
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
	logger *zap.Logger
	client *binance.BinanceHttpClient
}

func NewBinanceClient(cfg *binance.BinanceHttpClientConfig) *BinanceClient {
	return &BinanceClient{
		logger: log.GetLogger("BinanceClient"),
		client: binance.NewBinanceHttpClient(cfg),
	}
}

func (s BinanceClient) GetFullSnapshot(ctx context.Context, pair string, depth int) ([]model.DepthSnapshotPart, error) {
	s.logger.Info(fmt.Sprintf("get full shapshot [%s]", pair))
	snapshot, err := s.client.GetFullSnapshot(ctx, bmodel.Symbol(pair), depth)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	timestamp := time.Now().UnixMilli()
	var snapshotParts []model.DepthSnapshotPart
	for _, bid := range snapshot.Bids {
		snapshotParts = append(snapshotParts, model.NewDepthSnapshotPart(snapshot.LastUpdateId, true, bid[0], bid[1], model.SymbolFromString(pair), timestamp))
	}
	for _, ask := range snapshot.Asks {
		snapshotParts = append(snapshotParts, model.NewDepthSnapshotPart(snapshot.LastUpdateId, false, ask[0], ask[1], model.SymbolFromString(pair), timestamp))
	}
	return snapshotParts, nil
}

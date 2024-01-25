package web

import (
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"go.uber.org/zap"
	"strconv"
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
	snapshot, err := s.client.GetFullSnapshot(ctx, bmodel.Symbol(pair), depth)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	var snapshotParts []model.DepthSnapshotPart
	for _, bid := range snapshot.Bids {
		price, err := strconv.ParseFloat(bid[0], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		count, err := strconv.ParseFloat(bid[1], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		snapshotParts = append(snapshotParts, model.DepthSnapshotPart{
			LastUpdateId: snapshot.LastUpdateId,
			T:            true,
			Price:        price,
			Count:        count,
		})
	}
	for _, ask := range snapshot.Asks {
		price, err := strconv.ParseFloat(ask[0], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		count, err := strconv.ParseFloat(ask[1], 64)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		snapshotParts = append(snapshotParts, model.DepthSnapshotPart{
			LastUpdateId: snapshot.LastUpdateId,
			T:            false,
			Price:        price,
			Count:        count,
		})
	}
	return snapshotParts, nil
}

package svc

import (
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
)

type BookTicksWorkerProvider struct {
	cfg              *binance.BinanceHttpClientConfig
	dataType         string
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick]
	batchSize        int
	dataStorages     []BatchedDataStorage[bmodel.SymbolTick]
	metrics          WsDataPipelineMetrics[bmodel.SymbolTick]
}

func NewBookTicksWorkerProvider(
	cfg *binance.BinanceHttpClientConfig,
	dataType string,
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick],
	batchSize int,
	dataStorages []BatchedDataStorage[bmodel.SymbolTick],
	metrics WsDataPipelineMetrics[bmodel.SymbolTick],
) *BookTicksWorkerProvider {
	return &BookTicksWorkerProvider{
		cfg:              cfg,
		dataType:         dataType,
		dataTrasformator: dataTrasformator,
		batchSize:        batchSize,
		dataStorages:     dataStorages,
		metrics:          metrics,
	}
}

func (s BookTicksWorkerProvider) GetNewWorkers(ctx context.Context, symbols []string) *WsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick] {
	ticksReceiver := binance.NewBookTickerClient(s.cfg, symbols)
	return NewWsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick](s.dataType, ticksReceiver, s.dataTrasformator, s.batchSize, s.dataStorages, s.metrics)
}

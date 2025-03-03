package svc

import (
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
)

type BookTicksWorkerProvider struct {
	cfg              *binance.BinanceHttpClientConfig
	dataType         string
	marketType       bmodel.DataType
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick]
	batchSize        int
	dataStorages     []BatchedDataStorage[bmodel.SymbolTick]
	metrics          WsDataPipelineMetrics[bmodel.SymbolTick]
}

func NewBookTicksWorkerProvider(
	cfg *binance.BinanceHttpClientConfig,
	dataType string,
	marketType bmodel.DataType,
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick],
	batchSize int,
	dataStorages []BatchedDataStorage[bmodel.SymbolTick],
	metrics WsDataPipelineMetrics[bmodel.SymbolTick],
) *BookTicksWorkerProvider {
	return &BookTicksWorkerProvider{
		cfg:              cfg,
		dataType:         dataType,
		marketType:       marketType,
		dataTrasformator: dataTrasformator,
		batchSize:        batchSize,
		dataStorages:     dataStorages,
		metrics:          metrics,
	}
}

func (s BookTicksWorkerProvider) GetNewWorkers(ctx context.Context, symbols []string) *WsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick] {
	ticksReceiver := binance.NewBookTickerClient(s.cfg, symbols)
	return NewWsDataProcessWorker(s.dataType, ticksReceiver, s.dataTrasformator, s.batchSize, s.dataStorages, s.metrics)
}

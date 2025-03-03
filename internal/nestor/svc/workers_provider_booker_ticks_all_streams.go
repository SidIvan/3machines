package svc

import (
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
)

type BookTicksAllStreamsWorkerProvider struct {
	cfg              *binance.BinanceHttpClientConfig
	dataType         string
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick]
	batchSize        int
	dataStorages     []BatchedDataStorage[bmodel.SymbolTick]
	metrics          WsDataPipelineMetrics[bmodel.SymbolTick]
}

func NewBookTicksAllStreamsWorkerProvider(
	cfg *binance.BinanceHttpClientConfig,
	dataType string,
	dataTrasformator DataTransformator[bmodel.SymbolTick, bmodel.SymbolTick],
	batchSize int,
	dataStorages []BatchedDataStorage[bmodel.SymbolTick],
	metrics WsDataPipelineMetrics[bmodel.SymbolTick],
) *BookTicksAllStreamsWorkerProvider {
	return &BookTicksAllStreamsWorkerProvider{
		cfg:              cfg,
		dataType:         dataType,
		dataTrasformator: dataTrasformator,
		batchSize:        batchSize,
		dataStorages:     dataStorages,
		metrics:          metrics,
	}
}

func (s *BookTicksAllStreamsWorkerProvider) GetNewWorkers(ctx context.Context) []*WsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick] {
	return []*WsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick]{NewWsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick](
		s.dataType,
		binance.NewBookTickerClient(s.cfg, []string{}),
		s.dataTrasformator,
		s.batchSize,
		s.dataStorages,
		s.metrics,
	)}
}

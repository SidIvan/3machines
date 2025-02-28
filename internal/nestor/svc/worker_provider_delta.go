package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
)

type DeltaWorkerProvider struct {
	cfg              *binance.BinanceHttpClientConfig
	dataType         string
	dataTrasformator DataTransformator[bmodel.DeltaMessage, model.Delta]
	batchSize        int
	dataStorages     []BatchedDataStorage[model.Delta]
	metrics          WsDataPipelineMetrics[model.Delta]
}

func NewDeltaWorkerProvider(
	cfg *binance.BinanceHttpClientConfig,
	dataType string,
	dataTrasformator DataTransformator[bmodel.DeltaMessage, model.Delta],
	batchSize int,
	dataStorages []BatchedDataStorage[model.Delta],
	metrics WsDataPipelineMetrics[model.Delta],
) *DeltaWorkerProvider {
	return &DeltaWorkerProvider{
		cfg:              cfg,
		dataType:         dataType,
		dataTrasformator: dataTrasformator,
		batchSize:        batchSize,
		dataStorages:     dataStorages,
		metrics:          metrics,
	}
}

func (s DeltaWorkerProvider) getNewWorkers(ctx context.Context, symbols []string) *WsDataProcessWorker[bmodel.DeltaMessage, model.Delta] {
	deltaReceiver := binance.NewDeltaReceiveClient(s.cfg, symbols)
	return NewWsDataProcessWorker[bmodel.DeltaMessage, model.Delta](s.dataType, deltaReceiver, s.dataTrasformator, s.batchSize, s.dataStorages, s.metrics)
}

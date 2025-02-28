package svc

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BinanceClient interface {
	GetFullSnapshot(ctx context.Context, symbol string, depth int) ([]model.DepthSnapshotPart, string, error)
	GetFullExchangeInfo(context.Context) (*bmodel.ExchangeInfo, error)
	GetBookTicks(ctx context.Context) ([]bmodel.SymbolTick, error)
}

type WsDataWorkersProvider[T any] interface {
	getNewWorkers(context.Context) []*T
}

type TradingSymbolsWorkerProvider[T any] interface {
	getNewWorkers(context.Context, []string) *T
}

type DataReceiver[T any] interface {
	ConnectWs(context.Context) error
	Recv(context.Context) (T, error)
	Shutdown(context.Context)
}

type DataTransformator[TFrom, TTo any] interface {
	Transform(TFrom) ([]TTo, error)
}

type BatchedDataStorage[T any] interface {
	Save(context.Context, []T) error
}

type AuxBatchedDataStorage[T any] interface {
	GetWithDeleteCallback(context.Context) ([]T, error, func() error)
}

type WsDataPipelineMetrics[T any] interface {
	IncStartedSaveGoroutines()
	IncEndedSaveGoroutines()
	ProcessDataMetrics([]T, TypeOfEvent)
	IncRecvErr()
}

type DeltaStorage interface {
	SendDeltas(context.Context, []model.Delta) error
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

type ExchangeInfoStorage interface {
	SendExchangeInfo(context.Context, *model.ExchangeInfo) error
	GetLastExchangeInfo(context.Context) *model.ExchangeInfo
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

type LocalRepo interface {
	SaveDeltas(context.Context, []model.Delta) error
	SaveSnapshot(context.Context, []model.DepthSnapshotPart) error
	SaveExchangeInfo(context.Context, *bmodel.ExchangeInfo) error
	SaveBookTicker(context.Context, []bmodel.SymbolTick) error
	GetDeltas(ctx context.Context, numDeltas int64) []model.DeltaWithId
	DeleteDeltas(ctx context.Context, ids []primitive.ObjectID) (int64, error)
	Connect(ctx context.Context) error
}

type TypeOfEvent int

const (
	Receive TypeOfEvent = iota
	Send
	Save
)

type MetricsHolder interface {
	ProcessSnapshotMetrics([]model.DepthSnapshotPart, TypeOfEvent)
	ProcessExInfoMetrics(TypeOfEvent)
	UpdateMetrics([]bmodel.SymbolInfo)
}

type Fixer interface {
	Fix()
}

type DeltaHolesStorage interface {
	SaveDeltaHole(context.Context, model.DeltaHole) error
}

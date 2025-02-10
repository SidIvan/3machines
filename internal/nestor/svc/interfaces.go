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

type DeltaStorage interface {
	SendDeltas(context.Context, []model.Delta) error
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

type SnapshotStorage interface {
	SendSnapshot(context.Context, []model.DepthSnapshotPart) error
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

type ExchangeInfoStorage interface {
	SendExchangeInfo(context.Context, *bmodel.ExchangeInfo) error
	GetLastExchangeInfo(context.Context) *bmodel.ExchangeInfo
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

type BookTicksStorage interface {
	SendBookTicks(context.Context, []bmodel.SymbolTick) error
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
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
	Reconnect(ctx context.Context)
}

type TypeOfEvent int

const (
	Receive TypeOfEvent = iota
	Send
	Save
)

type MetricsHolder interface {
	ProcessDeltaMetrics([]model.Delta, TypeOfEvent)
	ProcessTickMetrics([]bmodel.SymbolTick, TypeOfEvent)
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

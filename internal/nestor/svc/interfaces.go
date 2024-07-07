package svc

import (
	"DeltaReceiver/internal/common/model"
	nmodel "DeltaReceiver/internal/nestor/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BinanceClient interface {
	GetFullSnapshot(ctx context.Context, symbol string, depth int) ([]nmodel.DepthSnapshotPart, string, error)
	GetFullExchangeInfo(context.Context) (*bmodel.ExchangeInfo, error)
	GetBookTicks(ctx context.Context) ([]bmodel.SymbolTick, error)
}

type LocalRepo interface {
	SaveDeltas(context.Context, []model.Delta) error
	SaveSnapshot(context.Context, []nmodel.DepthSnapshotPart) error
	SaveExchangeInfo(context.Context, *bmodel.ExchangeInfo) error
	SaveBookTicker(context.Context, []bmodel.SymbolTick) error
	GetDeltas(ctx context.Context, numDeltas int64) []model.DeltaWithId
	DeleteDeltas(ctx context.Context, ids []primitive.ObjectID) (int64, error)
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context)
}

type GlobalRepo interface {
	SendSnapshot(context.Context, []nmodel.DepthSnapshotPart) error
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
	SendFullExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) error
	GetLastFullExchangeInfoHash(ctx context.Context) uint64
	GetLastFullExchangeInfo(ctx context.Context) *bmodel.ExchangeInfo
	SendBookTicks(context.Context, []bmodel.SymbolTick) error
	Disconnect(ctx context.Context)
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
	ProcessSnapshotMetrics([]nmodel.DepthSnapshotPart, TypeOfEvent)
	ProcessExInfoMetrics(TypeOfEvent)
	ProcessSystemMetrics()
	UpdateMetrics([]bmodel.SymbolInfo)
}

type Fixer interface {
	Fix()
}

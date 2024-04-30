package svc

import (
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
	"time"
)

type BinanceClient interface {
	GetFullSnapshot(ctx context.Context, symbol string, depth int) ([]model.DepthSnapshotPart, string, error)
	GetFullExchangeInfo(context.Context) (*bmodel.ExchangeInfo, error)
	GetBookTicks(ctx context.Context) ([]bmodel.SymbolTick, error)
}

type LocalRepo interface {
	SaveDeltas(context.Context, []model.Delta) error
	SaveSnapshot(context.Context, []model.DepthSnapshotPart) error
	SaveExchangeInfo(context.Context, *bmodel.ExchangeInfo) error
	SaveBookTicker(context.Context, []bmodel.SymbolTick) error
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context)
}

type GlobalRepo interface {
	GetLastSavedTimestamp(context.Context, model.Symbol) time.Time
	SendDeltas(context.Context, []model.Delta) error
	SendSnapshot(context.Context, []model.DepthSnapshotPart) error
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
	ProcessSnapshotMetrics([]model.DepthSnapshotPart, TypeOfEvent)
	ProcessExInfoMetrics(event TypeOfEvent)
	ProcessSystemMetrics()
	UpdateMetrics([]bmodel.SymbolInfo)
}

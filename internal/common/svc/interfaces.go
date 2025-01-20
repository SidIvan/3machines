package svc

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
	"errors"
	"time"
)

var (
	EmptyStorage = errors.New("empty table")
)

type TimePair struct {
	Earliest time.Time
	Latest   time.Time
}

type DeltaStorage interface {
	SendDeltas(context.Context, []model.Delta) error
	GetDeltas(ctx context.Context, symbol, deltaType string, fromTime, toTime time.Time) ([]model.Delta, error)
	DeleteDeltas(ctx context.Context, symbol string, fromTime, toTime time.Time) error
	GetTsSegment(ctx context.Context, since time.Time) (map[string]TimePair, error)
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

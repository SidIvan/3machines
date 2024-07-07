package svc

import (
	"DeltaReceiver/internal/common/model"
	"context"
	"errors"
	"time"
)

var EmptyStorage = errors.New("empty table")

type DeltaStorage interface {
	SendDeltas(context.Context, []model.Delta) error
	GetSymbol(ctx context.Context, fromTsS, toTsS int64, excludedSymbols map[string]struct{}) (string, error)
	GetDeltas(ctx context.Context, symbol string, fromTsS, toTsS int64) ([]model.Delta, error)
	DeleteDeltas(ctx context.Context, symbol string, fromTsS, toTsS int64, deltaType string) error
	GetEarliestTs(ctx context.Context) (time.Time, error)
	Connect(ctx context.Context) error
	Reconnect(ctx context.Context) error
	Disconnect(ctx context.Context)
}

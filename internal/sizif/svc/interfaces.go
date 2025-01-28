package svc

import (
	"DeltaReceiver/internal/common/model"
	"context"
)

type ParquetStorage[T any] interface {
	Save(context.Context, []T, *model.ProcessingKey) error
}

type SocratesStorage[T any] interface {
	GetKeys(context.Context) ([]model.ProcessingKey, error)
	Get(context.Context, *model.ProcessingKey) ([]T, error)
	Delete(context.Context, *model.ProcessingKey) error
}

type DataTransformator[T any] interface {
	Transform([]T, *model.ProcessingKey) ([]T, bool)
}

type LockOpStatus int8

const (
	LockedSuccessfully LockOpStatus = 0
	AlreadyLocked      LockOpStatus = 1
)

type KeyLocker interface {
	Lock(context.Context, *model.ProcessingKey) (LockOpStatus, error)
	MarkProcessed(context.Context, *model.ProcessingKey) error
}

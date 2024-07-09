package svc

import (
	"DeltaReceiver/internal/common/model"
	"errors"
)

var (
	ParquetAlreadyExists = errors.New("parquet already exists")
)

type ParquetStorage interface {
	SaveDeltas(deltas []model.Delta, key *ProcessingKey) error
	IsParquetExists(key *ProcessingKey) bool
}

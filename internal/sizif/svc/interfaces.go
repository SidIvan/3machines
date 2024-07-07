package svc

import (
	"DeltaReceiver/internal/common/model"
)

type ParquetStorage interface {
	SaveDeltas(deltas []model.Delta, key *ProcessingKey) error
	IsParquetExists(key *ProcessingKey) bool
}

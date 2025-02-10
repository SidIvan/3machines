package cs

import (
	"DeltaReceiver/internal/common/model"

	"github.com/gocql/gocql"
)

type InsertQueryBuilder[T any] interface {
	BuildQuery(*gocql.Batch, model.ProcessingKey, T)
}

type InsertKeyQueryBuilder interface {
	BuildQuery(*gocql.Batch, model.ProcessingKey)
}

type CsStorageMetrics interface {
	IncErrCount()
	UpdInsertDataBatchLatency(int64)
	UpdInsertQueryLatency(int64)
}

package b2

import (
	"DeltaReceiver/internal/common/model"
	"context"

	"github.com/Backblaze/blazer/b2"
)

type B2DeltaParquetStorage struct {
	bucket *b2.Bucket
}

func (s B2DeltaParquetStorage) Save(ctx context.Context, deltas []model.Delta, key *model.ProcessingKey) error {
	s.bucket.CreateKey()
}

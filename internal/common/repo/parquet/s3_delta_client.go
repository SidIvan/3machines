package parquet

import (
	"go.uber.org/zap"
)

type S3DeltaClient struct {
	logger      *zap.Logger
	s3Storage   *S3ParquetStorage
	baseDirName *string
}

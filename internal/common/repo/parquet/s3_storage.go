package parquet

import (
	"DeltaReceiver/internal/common/conf"
	"DeltaReceiver/pkg/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

type S3ParquetStorage struct {
	logger *zap.Logger
	Client *s3.S3
}

func NewS3ParquetStorage(cfg *conf.S3Cfg) *S3ParquetStorage {
	s3Uri := cfg.URICfg.GetEndpoint()
	session := session.Must(session.NewSession())
	creds := credentials.NewStaticCredentials(cfg.Id, cfg.Secret, "")
	region := cfg.Region
	aswconf := &aws.Config{
		Endpoint:    &s3Uri,
		Credentials: creds,
		Region:      &region,
	}
	cl := s3.New(session, aswconf)
	return &S3ParquetStorage{
		logger: log.GetLogger("S3ParquetStorage"),
		Client: cl,
	}
}

func (s *S3ParquetStorage) GetObject(input *s3.GetObjectInput) *s3.GetObjectOutput {
	out, err := s.Client.GetObject(input)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	return out
}

func (s *S3ParquetStorage) ListObjects(input *s3.ListObjectsV2Input) *s3.ListObjectsV2Output {
	out, err := s.Client.ListObjectsV2(input)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	return out
}

func (s *S3ParquetStorage) ListBuckets() []*s3.Bucket {
	buckets, err := s.Client.ListBuckets(nil)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	return buckets.Buckets
}

// func (s *S3ParquetStorage) IsParquetExists() {
// 	s.client.ListObjectsV2(
// 		&s3.ListObjectsV2Input{

// 		}
// 	)
// }

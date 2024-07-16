package parquet

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/sizif/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	xs3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

type S3ParquetStorage struct {
	logger      *zap.Logger
	client      *s3.S3
	bucketName  *string
	baseDirName *string
}

func NewS3ParquetStorage(bucketName string) *S3ParquetStorage {
	s3Uri := "https://s3.eu-central-003.backblazeb2.com"
	region := "eu-central-003"
	mySession := session.Must(session.NewSession())
	aswconf := &aws.Config{
		Endpoint:    &s3Uri,
		Credentials: credentials.NewStaticCredentials("00349cd200921250000000002", "K003JiFEMguIasIvzhZK8Lxk9yPUsps", ""),
		Region:      &region,
	}
	cl := s3.New(mySession, aswconf)
	return &S3ParquetStorage{
		logger:      log.GetLogger("LocalParquetStorage"),
		bucketName:  aws.String(bucketName),
		client:      cl,
		baseDirName: aws.String("binance/deltas"),
	}
}

func (s S3ParquetStorage) GetParquetPath(key *svc.ProcessingKey) *string {
	return aws.String(fmt.Sprintf("%s/%s/%s.Parquet", s.baseDirName, key.Symbol, key.DateTimeStart))
}

func (s S3ParquetStorage) SaveDeltas(deltas []model.Delta, key *svc.ProcessingKey) error {
	if s.IsParquetExists(key) {
		return ParquetAlreadyExists
	}
	fileWriter, err := xs3.NewS3FileWriterWithClient(context.TODO(), s.client, *s.bucketName, *s.GetParquetPath(key), nil)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	pWriter, err := writer.NewParquetWriter(fileWriter, new(model.Delta), 4)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	pWriter.CompressionType = parquet.CompressionCodec_BROTLI
	pWriter.PageSize = 128 * 1024 * 1024
	for _, delta := range deltas {
		for i := 0; i < 3; i++ {
			err = pWriter.Write(delta)
			if err == nil {
				break
			}
		}
	}
	if err = pWriter.WriteStop(); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	_ = fileWriter.Close()
	return nil
}

func (s S3ParquetStorage) IsParquetExists(key *svc.ProcessingKey) bool {
	out, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: s.bucketName,
		Prefix: s.GetParquetPath(key),
	})
	if err != nil {
		s.logger.Error(err.Error())
		return true
	}
	return *out.KeyCount > 0
}

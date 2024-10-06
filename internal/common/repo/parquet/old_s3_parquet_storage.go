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

type OldS3ParquetStorage struct {
	logger      *zap.Logger
	client      *s3.S3
	bucketName  *string
	baseDirName *string
}

func NewOldS3ParquetStorage(bucketName string) *OldS3ParquetStorage {
	s3Uri := "https://s3.eu-central-003.backblazeb2.com"
	region := "eu-central-003"
	mySession := session.Must(session.NewSession())
	aswconf := &aws.Config{
		Endpoint:    &s3Uri,
		Credentials: credentials.NewStaticCredentials("00349cd200921250000000002", "K003JiFEMguIasIvzhZK8Lxk9yPUsps", ""),
		Region:      &region,
	}
	cl := s3.New(mySession, aswconf)
	return &OldS3ParquetStorage{
		logger:      log.GetLogger("S3ParquetStorage"),
		bucketName:  aws.String(bucketName),
		client:      cl,
		baseDirName: aws.String("binance/deltas"),
	}
}

func (s OldS3ParquetStorage) GetParquetPath(key *svc.ProcessingKey) *string {
	s.logger.Debug(fmt.Sprintf("parquet path %s", fmt.Sprintf("%s/%s/%s.Parquet", *s.baseDirName, key.Symbol, key.DateTimeStart)))
	return aws.String(fmt.Sprintf("%s/%s/%s.Parquet", *s.baseDirName, key.Symbol, key.DateTimeStart))
}

func (s OldS3ParquetStorage) SaveDeltas(deltas []model.Delta, key *svc.ProcessingKey) error {
	// if s.IsParquetExists(key) {
	// 	s.logger.Info("parquet already exists")
	// 	return ParquetAlreadyExists
	// }
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
	pWriter.CompressionType = parquet.CompressionCodec_LZ4
	pWriter.PageSize = 128 * 1024 * 1024
	s.logger.Debug(fmt.Sprintf("try to save %d deltas", len(deltas)))
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
	s.logger.Info(fmt.Sprintf("parquet %s saved", *key))
	return nil
}

func (s OldS3ParquetStorage) IsParquetExists(key *svc.ProcessingKey) bool {
	out, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: s.bucketName,
		Prefix: s.GetParquetPath(key),
	})
	if err != nil {
		s.logger.Error(err.Error())
		return true
	}
	s.logger.Debug(fmt.Sprintf("found %d docs", *out.KeyCount))
	return *out.KeyCount > 0
}

package b2

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Backblaze/blazer/b2"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"
)

type B2ParquetStorage[T any] struct {
	logger              *zap.Logger
	bucket              *b2.Bucket
	storageName         string
	parquetWriterConfig *parquet.WriterConfig
}

func NewB2ParquetStorage[T any](bucket *b2.Bucket, storageName string) *B2ParquetStorage[T] {
	logger := log.GetLogger("B2ParquetStorage_" + storageName)
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = &parquet.Lz4Raw
	return &B2ParquetStorage[T]{
		logger:              logger,
		bucket:              bucket,
		storageName:         storageName,
		parquetWriterConfig: cfg,
	}
}

func (s B2ParquetStorage[T]) Save(ctx context.Context, entries []T, key *model.ProcessingKey) error {
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buffer, s.parquetWriterConfig)
	_, err := writer.Write(entries)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	err = writer.Close()
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	objWriter := s.bucket.Object(s.createObjKey(key)).NewWriter(ctx)
	_, err = io.Copy(objWriter, &buffer)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	err = objWriter.Close()
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s *B2ParquetStorage[T]) createObjKey(key *model.ProcessingKey) string {
	processingTime := time.Unix(key.HourNo*60*60, 0).UTC()
	keyDate := processingTime.Format("2006-01-02")
	keyTime := processingTime.Format("15-04-05")
	return fmt.Sprintf("%s/%s/%s/%s.parquet", s.storageName, key.Symbol, keyDate, keyTime)
}

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

type KeyTsFormType int

const (
	FromData = KeyTsFormType(0)
	FromKey  = KeyTsFormType(1)
)

type B2ParquetStorage[T any] struct {
	logger              *zap.Logger
	bucket              *b2.Bucket
	storageName         string
	parquetWriterConfig *parquet.WriterConfig
	keyTsFormType       KeyTsFormType
}

func NewB2ParquetStorage[T any](bucket *b2.Bucket, storageName string, keyTsFormType KeyTsFormType) *B2ParquetStorage[T] {
	logger := log.GetLogger("B2ParquetStorage_" + storageName)
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = &parquet.Lz4Raw
	return &B2ParquetStorage[T]{
		logger:              logger,
		bucket:              bucket,
		storageName:         storageName,
		parquetWriterConfig: cfg,
		keyTsFormType:       keyTsFormType,
	}
}

func (s B2ParquetStorage[T]) Save(ctx context.Context, entries []T, timestampMs int64, key *model.ProcessingKey) error {
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
	objKey := s.createObjKey(key, timestampMs)
	objWriter := s.bucket.Object(objKey).NewWriter(ctx)
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
	s.logger.Info(fmt.Sprintf("key %s saved", objKey))
	return nil
}

func (s *B2ParquetStorage[T]) createObjKey(key *model.ProcessingKey, timestampMs int64) string {
	var processingTime time.Time
	if s.keyTsFormType == FromData {
		processingTime = time.UnixMilli(timestampMs).UTC()
	} else if s.keyTsFormType == FromKey {
		processingTime = time.Unix(key.HourNo*60*60, 0).UTC()
	} else {
		panic("invalid key form ts type")
	}
	keyDate := processingTime.Format("2006-01-02")
	keyTime := processingTime.Format("15-04-05")
	return fmt.Sprintf("%s/%s/%s/%s.parquet", s.storageName, key.Symbol, keyDate, keyTime)
}

package parquet

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/sizif/svc"
	"DeltaReceiver/pkg/log"
	"errors"
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

var (
	InvalidDeltasBatchForSavingToParquet = errors.New("invalid deltas batch for saving to parquet")
	EmptyDeltasBatch                     = errors.New("empty deltas batch")
	ParquetAlreadyExists                 = errors.New("parquet already exists")
)

var dateTimeLayout = "2006-01-02"

type LocalParquetStorage struct {
	logger      *zap.Logger
	DirFullPath string
	mut         *sync.Mutex
}

func NewLocalParquetStorage(dirFullPath string) *LocalParquetStorage {
	var mut sync.Mutex
	return &LocalParquetStorage{
		logger:      log.GetLogger("LocalParquetStorage"),
		DirFullPath: dirFullPath,
		mut:         &mut,
	}
}

func validateDeltasForSaving(deltas []model.Delta, key *svc.ProcessingKey) error {
	if len(deltas) == 0 {
		return EmptyDeltasBatch
	}
	for _, delta := range deltas {
		if model.StringOfDeltaType(delta.T) != key.Type || delta.Symbol != key.Symbol || time.UnixMilli(delta.Timestamp).Format(dateTimeLayout) != key.Date {
			return InvalidDeltasBatchForSavingToParquet
		}
	}
	return nil
}

func (s LocalParquetStorage) GetParquetPath(key *svc.ProcessingKey) string {
	return fmt.Sprintf("/binance/%s/%s/%s/%s.parquet", s.DirFullPath, key.Type, key.Symbol, key.Date)
}

func (s LocalParquetStorage) SaveDeltas(deltas []model.Delta, key *svc.ProcessingKey) error {
	if err := validateDeltasForSaving(deltas, key); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.IsParquetExists(key) {
		return ParquetAlreadyExists
	}
	fout, err := os.Create(s.GetParquetPath(key))
	defer func(fout *os.File) {
		_ = fout.Close()
	}(fout)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	pWriter, err := writer.NewParquetWriterFromWriter(fout, new(model.Delta), 4)
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
		if err != nil {
			s.logger.Error(err.Error())
			_ = fout.Close()
			_ = os.Remove(fout.Name())
			return err
		}
	}
	if err = pWriter.WriteStop(); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s LocalParquetStorage) IsParquetExists(key *svc.ProcessingKey) bool {
	_, err := os.OpenFile(s.GetParquetPath(key), os.O_RDONLY, 0444)
	return err == nil
}

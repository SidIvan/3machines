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
	os.MkdirAll(dirFullPath, 0600)
	return &LocalParquetStorage{
		logger:      log.GetLogger("LocalParquetStorage"),
		DirFullPath: dirFullPath,
		mut:         &mut,
	}
}

func (s LocalParquetStorage) GetParquetPath(key *svc.ProcessingKey) string {
	os.MkdirAll(fmt.Sprintf("%s/%s", s.DirFullPath, key.Symbol), 0600)
	return fmt.Sprintf("%s/%s/%s.Parquet", s.DirFullPath, key.Symbol, key.DateTimeStart)
}

func (s LocalParquetStorage) SaveDeltas(deltas []model.Delta, key *svc.ProcessingKey) error {
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

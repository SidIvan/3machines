package cs

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsSnapshotStorage struct {
	logger          *zap.Logger
	session         *gocql.Session
	tableName       string
	insertStatement string
}

func NewCsSnapshotStorage(session *gocql.Session, tableName string) *CsSnapshotStorage {
	logger := log.GetLogger("CsSnapshotStorage")
	snapshotStorage := &CsSnapshotStorage{
		logger:    logger,
		session:   session,
		tableName: tableName,
	}
	snapshotStorage.initStatements()
	return snapshotStorage
}

func (s *CsSnapshotStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, last_update_id) VALUES (?, ?, ?, ?, ?, ?, ?)", s.tableName)
}

func (s CsSnapshotStorage) SendSnapshot(ctx context.Context, snapshotParts []model.DepthSnapshotPart) error {
	var wg sync.WaitGroup
	var numSuccessInserts atomic.Int32
	numInserts := 0
	for i := 0; i < len(snapshotParts); i += batchSize {
		numInserts++
		wg.Add(1)
		go func(batch []model.DepthSnapshotPart) error {
			defer wg.Done()
			err := s.sendSnapshotMicroBatch(ctx, batch)
			if err != nil {
				s.logger.Error(err.Error())
				return err
			}
			numSuccessInserts.Add(1)
			return nil
		}(snapshotParts[i:min(len(snapshotParts), i+batchSize)])
	}
	wg.Wait()
	if numSuccessInserts.Load() == int32(numInserts) {
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsSnapshotStorage) sendSnapshotMicroBatch(ctx context.Context, snapshotParts []model.DepthSnapshotPart) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, snapshotPart := range snapshotParts {
		batch.Query(s.insertStatement, snapshotPart.Symbol, getHourNo(snapshotPart.Timestamp), snapshotPart.Timestamp, snapshotPart.T, snapshotPart.Price, snapshotPart.Count, snapshotPart.LastUpdateId)
	}
	err := s.session.ExecuteBatch(batch)
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsSnapshotStorage) Connect(ctx context.Context) error {
	return nil
}

func (s CsSnapshotStorage) Reconnect(ctx context.Context) error {
	return nil
}

func (s CsSnapshotStorage) Disconnect(ctx context.Context) {
	if !s.session.Closed() {
		s.session.Close()
	}
}

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

var sentSnapshotsKeysMut sync.Mutex
var sentSnapshotsKeys = make(map[model.ProcessingKey]struct{})

type CsSnapshotStorage struct {
	logger             *zap.Logger
	session            *gocql.Session
	tableName          string
	keysTableName      string
	insertStatement    string
	insertKeyStatement string
}

func NewCsSnapshotStorage(session *gocql.Session, tableName string, keysTableName string) *CsSnapshotStorage {
	logger := log.GetLogger("CsSnapshotStorage")
	snapshotStorage := &CsSnapshotStorage{
		logger:        logger,
		session:       session,
		tableName:     tableName,
		keysTableName: keysTableName,
	}
	snapshotStorage.initStatements()
	return snapshotStorage
}

func (s *CsSnapshotStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, last_update_id) VALUES (?, ?, ?, ?, ?, ?, ?)", s.tableName)
	s.insertKeyStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour) VALUES (?, ?)", s.keysTableName)
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
		s.sendKeys(ctx, snapshotParts)
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsSnapshotStorage) sendKeys(ctx context.Context, deltas []model.DepthSnapshotPart) error {
	s.logger.Info("sending keys")
	var err error
	batchKeys := make(map[model.ProcessingKey]struct{})
	for _, delta := range deltas {
		deltaKey := model.ProcessingKey{
			Symbol: delta.Symbol,
			HourNo: GetHourNo(delta.Timestamp),
		}
		batchKeys[deltaKey] = struct{}{}
	}
	var newKeys []model.ProcessingKey
	sentSnapshotsKeysMut.Lock()
	for key, _ := range batchKeys {
		if _, ok := sentSnapshotsKeys[key]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	sentSnapshotsKeysMut.Unlock()
	var wg sync.WaitGroup
	var numSuccessInserts atomic.Int32
	numInserts := 0
	for j := 0; j < 3; j++ {
		for i := 0; i < len(newKeys); i += batchSize {
			numInserts++
			wg.Add(1)
			go func(keys []model.ProcessingKey) {
				if s.sendKeysBatch(ctx, keys) == nil {
					numSuccessInserts.Add(1)
				}
				wg.Done()
			}(newKeys[i:min(i, len(newKeys))])
		}
		wg.Wait()
		if numSuccessInserts.Load() == int32(numInserts) {
			return nil
		}
	}
	return err
}

func (s CsSnapshotStorage) sendKeysBatch(ctx context.Context, keys []model.ProcessingKey) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, key := range keys {
		batch.Query(s.insertKeyStatement, key.Symbol, key.HourNo)
	}
	err := s.session.ExecuteBatch(batch)
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsSnapshotStorage) sendSnapshotMicroBatch(ctx context.Context, snapshotParts []model.DepthSnapshotPart) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, snapshotPart := range snapshotParts {
		batch.Query(s.insertStatement, snapshotPart.Symbol, GetHourNo(snapshotPart.Timestamp), snapshotPart.Timestamp, snapshotPart.T, snapshotPart.Price, snapshotPart.Count, snapshotPart.LastUpdateId)
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

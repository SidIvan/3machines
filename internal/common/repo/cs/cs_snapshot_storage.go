package cs

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

var sentSnapshotsKeysMut sync.Mutex
var sentSnapshotsKeys = make(map[model.ProcessingKey]struct{})

type CsSnapshotStorage struct {
	logger              *zap.Logger
	session             *gocql.Session
	tableName           string
	keysTableName       string
	insertStatement     string
	insertKeyStatement  string
	selectStatement     string
	selectKeysStatement string
	deleteStatement     string
	deleteKeyStatement  string
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
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, type, price, count, last_update_id FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT (symbol, hour) FROM %s", s.keysTableName)
	s.deleteStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.keysTableName)
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
	for key := range batchKeys {
		if _, ok := sentSnapshotsKeys[key]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	sentSnapshotsKeysMut.Unlock()
	s.logger.Info(fmt.Sprintf("got %d new keys", len(newKeys)))
	for j := 0; j < 3; j++ {
		var wg sync.WaitGroup
		var numSuccessInserts atomic.Int32
		numInserts := 0
		for i := 0; i < len(newKeys); i += batchSize {
			numInserts++
			wg.Add(1)
			go func(keys []model.ProcessingKey) {
				if s.sendKeysBatch(ctx, keys) == nil {
					numSuccessInserts.Add(1)
				}
				wg.Done()
			}(newKeys[i:min(i+batchSize, len(newKeys))])
		}
		wg.Wait()
		if numSuccessInserts.Load() == int32(numInserts) {
			s.logger.Info(fmt.Sprintf("successfully insert %d new keys", len(newKeys)))
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

func (s CsSnapshotStorage) Get(ctx context.Context, key *model.ProcessingKey) ([]model.DepthSnapshotPart, error) {
	var snapshotPart model.DepthSnapshotPart
	var snapshotParts []model.DepthSnapshotPart
	startTime := time.Now()
	s.logger.Info(fmt.Sprintf("get snapshot parts for %s", key))
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&snapshotPart.Symbol, &snapshotPart.Timestamp, &snapshotPart.T, &snapshotPart.Price, &snapshotPart.Count, &snapshotPart.LastUpdateId) {
		snapshotParts = append(snapshotParts, snapshotPart)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	s.logger.Info(fmt.Sprintf("get deltas for %s took %d", key, time.Now().UnixMilli()-startTime.UnixMilli()))
	return snapshotParts, err
}

func (s CsSnapshotStorage) GetKeys(ctx context.Context) ([]model.ProcessingKey, error) {
	var key model.ProcessingKey
	var keys []model.ProcessingKey
	it := s.session.Query(s.selectKeysStatement).Consistency(gocql.All).WithContext(ctx).Iter()
	for it.Scan(&key.Symbol, &key.HourNo) {
		keys = append(keys, key)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return keys, err
}

func (s CsSnapshotStorage) Delete(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s *CsSnapshotStorage) DeleteKey(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteKeyStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
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

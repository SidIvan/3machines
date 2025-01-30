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

var sentKeysMut sync.Mutex
var sentKeys = make(map[model.ProcessingKey]struct{})

type CsDeltaStorage struct {
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

func NewCsDeltaStorage(session *gocql.Session, tableName string, keysTableName string) *CsDeltaStorage {
	logger := log.GetLogger("CsDeltaStorage")
	deltaStorage := &CsDeltaStorage{
		logger:        logger,
		session:       session,
		tableName:     tableName,
		keysTableName: keysTableName,
	}
	deltaStorage.initStatements()
	return deltaStorage
}

func (s *CsDeltaStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, first_update_id, update_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
	s.insertKeyStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour) VALUES (?, ?)", s.keysTableName)
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, type, price, count, first_update_id, update_id FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT (symbol, hour) FROM %s", s.keysTableName)
	s.deleteStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.keysTableName)
}

func (s CsDeltaStorage) SendDeltas(ctx context.Context, deltas []model.Delta) error {
	csInsertStart := time.Now()
	defer func() {
		now := time.Now()
		s.logger.Debug(fmt.Sprintf("inserting deltas from %d to %d got %d ms", csInsertStart.UnixMilli(), now.UnixMilli(), now.UnixMilli()-csInsertStart.UnixMilli()))
	}()
	var wg sync.WaitGroup
	var numSuccessInserts atomic.Int32
	numInserts := 0
	for i := 0; i < len(deltas); i += batchSize {
		numInserts++
		wg.Add(1)
		go func(batch []model.Delta) error {
			defer wg.Done()
			err := s.sendDeltasMicroBatch(ctx, batch)
			if err != nil {
				s.logger.Error(err.Error())
				return err
			}
			numSuccessInserts.Add(1)
			return nil
		}(deltas[i:min(len(deltas), i+batchSize)])
	}
	wg.Wait()
	if numSuccessInserts.Load() == int32(numInserts) {
		s.logger.Debug(fmt.Sprintf("batch of %d deltas inserted successfully", len(deltas)))
		s.sendKeys(ctx, deltas)
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsDeltaStorage) sendKeys(ctx context.Context, deltas []model.Delta) error {
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
	sentKeysMut.Lock()
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

func (s CsDeltaStorage) sendKeysBatch(ctx context.Context, keys []model.ProcessingKey) error {
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

func (s CsDeltaStorage) sendDeltasMicroBatch(ctx context.Context, deltas []model.Delta) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, delta := range deltas {
		batch.Query(s.insertStatement, delta.Symbol, GetHourNo(delta.Timestamp), delta.Timestamp, delta.T, delta.Price, delta.Count, delta.FirstUpdateId, delta.UpdateId)
	}
	err := s.session.ExecuteBatch(batch)
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsDeltaStorage) Get(ctx context.Context, key *model.ProcessingKey) ([]model.Delta, error) {
	var delta model.Delta
	var deltas []model.Delta
	startTime := time.Now()
	s.logger.Info(fmt.Sprintf("get deltas for %s", key))
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&delta.Symbol, &delta.Timestamp, &delta.T, &delta.Price, &delta.Count, &delta.FirstUpdateId, &delta.UpdateId) {
		deltas = append(deltas, delta)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	s.logger.Info(fmt.Sprintf("get deltas for %s took %d", key, time.Now().UnixMilli()-startTime.UnixMilli()))
	return deltas, err
}

func (s CsDeltaStorage) GetKeys(ctx context.Context) ([]model.ProcessingKey, error) {
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

func (s CsDeltaStorage) Delete(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s *CsDeltaStorage) DeleteKey(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteKeyStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsDeltaStorage) Connect(ctx context.Context) error {
	return nil
}

func (s CsDeltaStorage) Reconnect(ctx context.Context) error {
	return nil
}

func (s CsDeltaStorage) Disconnect(ctx context.Context) {
	s.session.Close()
}

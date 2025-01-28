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

type CsDeltaStorage struct {
	logger              *zap.Logger
	session             *gocql.Session
	tableName           string
	insertStatement     string
	selectStatement     string
	selectKeysStatement string
	deleteKeyStatement  string
}

func NewCsDeltaStorage(session *gocql.Session, tableName string) *CsDeltaStorage {
	logger := log.GetLogger("CsDeltaStorage")
	deltaStorage := &CsDeltaStorage{
		logger:    logger,
		session:   session,
		tableName: tableName,
	}
	deltaStorage.initStatements()
	return deltaStorage
}

func (s *CsDeltaStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, first_update_id, update_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, type, price, count, first_update_id, update_id FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT distinct (symbol, hour) FROM %s", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
}

func (s CsDeltaStorage) SendDeltas(ctx context.Context, deltas []model.Delta) error {
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
		return nil
	}
	return errors.New("batch not saved")
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
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&delta) {
		deltas = append(deltas, delta)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return deltas, err
}

func (s CsDeltaStorage) GetKeys(ctx context.Context) ([]model.ProcessingKey, error) {
	var key model.ProcessingKey
	var keys []model.ProcessingKey
	it := s.session.Query(s.selectKeysStatement).WithContext(ctx).Iter()
	for it.Scan(&key) {
		keys = append(keys, key)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return keys, err
}

func (s CsDeltaStorage) Delete(ctx context.Context, key *model.ProcessingKey) error {
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

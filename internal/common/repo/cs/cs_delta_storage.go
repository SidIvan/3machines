package cs

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsDeltaStorage struct {
	logger              *zap.Logger
	session             *gocql.Session
	metrics             CsStorageMetrics
	tableName           string
	dataUploader        *CsDataUploader[model.Delta]
	keysTableName       string
	selectStatement     string
	selectKeysStatement string
	deleteStatement     string
	deleteKeyStatement  string
}

func NewCsDeltaStorageWO(loggerParam string, session *gocql.Session, metrics CsStorageMetrics, tableName string, keysTableName string) *CsDeltaStorage {
	logger := log.GetLogger(fmt.Sprintf("CsDeltaStorage[%s]", loggerParam))
	deltaStorage := &CsDeltaStorage{
		logger:        logger,
		session:       session,
		metrics:       metrics,
		tableName:     tableName,
		dataUploader:  NewCsDataUploader(logger, session, metrics, keysTableName, NewDeltaInsertQueryBuilder(tableName)),
		keysTableName: keysTableName,
	}
	return deltaStorage
}

func NewCsDeltaStorageRO(session *gocql.Session, tableName string, keysTableName string) *CsDeltaStorage {
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
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, type, price, count, first_update_id, update_id FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT (symbol, hour) FROM %s", s.keysTableName)
	s.deleteStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.keysTableName)
}

func (s CsDeltaStorage) Save(ctx context.Context, deltas []model.Delta) error {
	return s.SendDeltas(ctx, deltas)
}

func (s CsDeltaStorage) SendDeltas(ctx context.Context, deltas []model.Delta) error {
	csInsertStart := time.Now()
	defer func() {
		now := time.Now()
		latencyMs := now.UnixMilli() - csInsertStart.UnixMilli()
		s.metrics.UpdInsertDataBatchLatency(latencyMs)
	}()
	err := s.dataUploader.UploadData(ctx, deltas)
	if err != nil {
		s.metrics.IncErrCount()
		s.logger.Error(err.Error())
		return errors.New("batch not saved")
	}
	return nil
}

func (s CsDeltaStorage) Get(ctx context.Context, key *model.ProcessingKey) ([]model.Delta, error) {
	var delta model.Delta
	var deltas []model.Delta
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&delta.Symbol, &delta.Timestamp, &delta.T, &delta.Price, &delta.Count, &delta.FirstUpdateId, &delta.UpdateId) {
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

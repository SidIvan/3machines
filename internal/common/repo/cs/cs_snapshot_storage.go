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

type CsSnapshotStorage struct {
	logger              *zap.Logger
	session             *gocql.Session
	metrics             CsStorageMetrics
	tableName           string
	keysTableName       string
	dataUploader        *CsDataUploader[model.DepthSnapshotPart]
	selectStatement     string
	selectKeysStatement string
	deleteStatement     string
	deleteKeyStatement  string
}

func NewCsSnapshotStorageWO(session *gocql.Session, metrics CsStorageMetrics, tableName string, keysTableName string) *CsSnapshotStorage {
	logger := log.GetLogger("CsSnapshotStorage")
	snapshotStorage := &CsSnapshotStorage{
		logger:        logger,
		session:       session,
		metrics:       metrics,
		tableName:     tableName,
		keysTableName: keysTableName,
		dataUploader:  NewCsDataUploader(logger, session, metrics, keysTableName, NewSnapshotInsertQueryBuilder(tableName)),
	}
	snapshotStorage.initStatements()
	return snapshotStorage
}

func NewCsSnapshotStorageRO(session *gocql.Session, tableName string, keysTableName string) *CsSnapshotStorage {
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
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, type, price, count, last_update_id FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT (symbol, hour) FROM %s", s.keysTableName)
	s.deleteStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.keysTableName)
}

func (s CsSnapshotStorage) Save(ctx context.Context, snapshotParts []model.DepthSnapshotPart) error {
	s.logger.Debug("SAVE MOCK")
	return nil
	return s.SendSnapshot(ctx, snapshotParts)
}

func (s CsSnapshotStorage) SendSnapshot(ctx context.Context, snapshotParts []model.DepthSnapshotPart) error {
	csInsertStart := time.Now()
	defer func() {
		now := time.Now()
		latencyMs := now.UnixMilli() - csInsertStart.UnixMilli()
		s.metrics.UpdInsertDataBatchLatency(latencyMs)
	}()
	err := s.dataUploader.UploadData(ctx, snapshotParts)
	if err != nil {
		s.metrics.IncErrCount()
		s.logger.Error(err.Error())
		return errors.New("batch not saved")
	}
	return nil
}

func (s CsSnapshotStorage) Get(ctx context.Context, key *model.ProcessingKey) ([]model.DepthSnapshotPart, error) {
	var snapshotPart model.DepthSnapshotPart
	var snapshotParts []model.DepthSnapshotPart
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&snapshotPart.Symbol, &snapshotPart.Timestamp, &snapshotPart.T, &snapshotPart.Price, &snapshotPart.Count, &snapshotPart.LastUpdateId) {
		snapshotParts = append(snapshotParts, snapshotPart)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
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

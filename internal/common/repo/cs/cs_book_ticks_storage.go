package cs

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsBookTicksStorage struct {
	logger              *zap.Logger
	session             *gocql.Session
	metrics             CsStorageMetrics
	tableName           string
	keysTableName       string
	dataUploader        *CsDataUploader[bmodel.SymbolTick]
	selectStatement     string
	selectKeysStatement string
	deleteStatement     string
	deleteKeyStatement  string
}

func NewCsBookTicksStorageWO(session *gocql.Session, metrics CsStorageMetrics, tableName string, keysTableName string) *CsBookTicksStorage {
	logger := log.GetLogger("CsBookTicksStorage")
	bookTicksStorage := &CsBookTicksStorage{
		logger:        logger,
		session:       session,
		metrics:       metrics,
		tableName:     tableName,
		keysTableName: keysTableName,
		dataUploader:  NewCsDataUploader(logger, session, metrics, keysTableName, (NewBookTicksInsertQueryBuilder(tableName))),
	}
	bookTicksStorage.initStatements()
	return bookTicksStorage
}

func NewCsBookTicksStorageRO(session *gocql.Session, tableName string, keysTableName string) *CsBookTicksStorage {
	logger := log.GetLogger("CsBookTicksStorage")
	bookTicksStorage := &CsBookTicksStorage{
		logger:        logger,
		session:       session,
		tableName:     tableName,
		keysTableName: keysTableName,
	}
	bookTicksStorage.initStatements()
	return bookTicksStorage
}

func (s *CsBookTicksStorage) initStatements() {
	s.selectStatement = fmt.Sprintf("SELECT symbol, timestamp_ms, update_id, ask_price, ask_quantity, bid_price, bid_quantity FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.selectKeysStatement = fmt.Sprintf("SELECT (symbol, hour) FROM %s", s.keysTableName)
	s.deleteStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.tableName)
	s.deleteKeyStatement = fmt.Sprintf("DELETE FROM %s WHERE symbol = ? AND hour = ?", s.keysTableName)
}

func (s CsBookTicksStorage) Save(ctx context.Context, deltas []bmodel.SymbolTick) error {
	return s.SendBookTicks(ctx, deltas)
}

func (s CsBookTicksStorage) SendBookTicks(ctx context.Context, deltas []bmodel.SymbolTick) error {
	csInsertStart := time.Now()
	defer func() {
		now := time.Now()
		latencyMs := now.UnixMilli() - csInsertStart.UnixMilli()
		s.logger.Debug(fmt.Sprintf("inserting book ticks from %d to %d got %d ms", csInsertStart.UnixMilli(), now.UnixMilli(), latencyMs))
		s.metrics.UpdInsertDataBatchLatency(latencyMs)
	}()
	err := s.dataUploader.UploadData(ctx, deltas)
	if err != nil {
		s.metrics.IncErrCount()
		s.logger.Error(err.Error())
		return errors.New("batch not saved")
	}
	s.logger.Debug(fmt.Sprintf("batch of %d deltas inserted successfully", len(deltas)))
	return nil
}

func (s CsBookTicksStorage) Get(ctx context.Context, key *model.ProcessingKey) ([]bmodel.SymbolTick, error) {
	var tick bmodel.SymbolTick
	var ticks []bmodel.SymbolTick
	startTime := time.Now()
	s.logger.Info(fmt.Sprintf("get ticks for %s", key))
	it := s.session.Query(s.selectStatement, key.Symbol, key.HourNo).WithContext(ctx).Iter()
	for it.Scan(&tick.Symbol, &tick.Timestamp, &tick.UpdateId, &tick.AskPrice, &tick.AskQuantity, &tick.BidPrice, &tick.BidQuantity) {
		ticks = append(ticks, tick)
	}
	err := it.Close()
	if err != nil {
		s.logger.Error(err.Error())
	}
	s.logger.Info(fmt.Sprintf("get ticks for %s took %d", key, time.Now().UnixMilli()-startTime.UnixMilli()))
	return ticks, err
}

func (s CsBookTicksStorage) GetKeys(ctx context.Context) ([]model.ProcessingKey, error) {
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

func (s CsBookTicksStorage) Delete(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s *CsBookTicksStorage) DeleteKey(ctx context.Context, key *model.ProcessingKey) error {
	var query = s.session.Query(s.deleteKeyStatement, key.Symbol, key.HourNo).WithContext(ctx)
	query.SetConsistency(gocql.All)
	err := query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsBookTicksStorage) Connect(ctx context.Context) error {
	return nil
}

func (s CsBookTicksStorage) Reconnect(ctx context.Context) error {
	return nil
}

func (s CsBookTicksStorage) Disconnect(ctx context.Context) {
	if !s.session.Closed() {
		s.session.Close()
	}
}

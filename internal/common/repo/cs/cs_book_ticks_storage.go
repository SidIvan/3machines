package cs

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

var sentTicksKeysMut sync.Mutex
var sentTicksKeys = make(map[model.ProcessingKey]struct{})

type CsBookTicksStorage struct {
	logger             *zap.Logger
	session            *gocql.Session
	tableName          string
	keysTableName      string
	insertStatement    string
	insertKeyStatement string
}

func NewCsBookTicksStorage(session *gocql.Session, tableName string, keysTableName string) *CsBookTicksStorage {
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
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, update_id, ask_price, ask_quantity, bid_price, bid_quantity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
	s.insertKeyStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour) VALUES (?, ?)", s.keysTableName)
}

func (s CsBookTicksStorage) SendBookTicks(ctx context.Context, bookTicks []bmodel.SymbolTick) error {
	var wg sync.WaitGroup
	var numSuccessInserts atomic.Int32
	numInserts := 0
	for i := 0; i < len(bookTicks); i += batchSize {
		numInserts++
		wg.Add(1)
		go func(batch []bmodel.SymbolTick) error {
			defer wg.Done()
			err := s.sendMicroBatch(ctx, batch)
			if err != nil {
				s.logger.Error(err.Error())
				return err
			}
			numSuccessInserts.Add(1)
			return nil
		}(bookTicks[i:min(len(bookTicks), i+batchSize)])
	}
	wg.Wait()
	if numSuccessInserts.Load() == int32(numInserts) {
		s.sendKeys(ctx, bookTicks)
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsBookTicksStorage) sendKeys(ctx context.Context, deltas []bmodel.SymbolTick) error {
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
	sentTicksKeysMut.Lock()
	for key, _ := range batchKeys {
		if _, ok := sentTicksKeys[key]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	sentTicksKeysMut.Unlock()
	s.logger.Info(fmt.Sprintf("got %d new keys", len(newKeys)))
	if len(newKeys) == 0 {
		s.logger.Info(fmt.Sprintf("no new keys"))
		return nil
	}
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

func (s CsBookTicksStorage) sendKeysBatch(ctx context.Context, keys []model.ProcessingKey) error {
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

func (s CsBookTicksStorage) sendMicroBatch(ctx context.Context, bookTicks []bmodel.SymbolTick) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, bookTick := range bookTicks {
		batch.Query(s.insertStatement, bookTick.Symbol, GetHourNo(bookTick.Timestamp), bookTick.Timestamp, bookTick.UpdateId, bookTick.AskPrice, bookTick.AskQuantity, bookTick.BidPrice, bookTick.BidQuantity)
	}
	err := s.session.ExecuteBatch(batch)
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

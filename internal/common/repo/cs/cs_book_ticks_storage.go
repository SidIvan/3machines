package cs

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsBookTicksStorage struct {
	logger          *zap.Logger
	session         *gocql.Session
	tableName       string
	insertStatement string
}

func NewCsBookTicksStorage(session *gocql.Session, tableName string) *CsBookTicksStorage {
	logger := log.GetLogger("CsBookTicksStorage")
	bookTicksStorage := &CsBookTicksStorage{
		logger:    logger,
		session:   session,
		tableName: tableName,
	}
	bookTicksStorage.initStatements()
	return bookTicksStorage
}

func (s *CsBookTicksStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, update_id, ask_price, ask_quantity, bid_price, bid_quantity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
}

func (s CsBookTicksStorage) SendBookTicks(ctx context.Context, bookTicks []model.SymbolTick) error {
	var wg sync.WaitGroup
	var numSuccessInserts atomic.Int32
	numInserts := 0
	for i := 0; i < len(bookTicks); i += batchSize {
		numInserts++
		wg.Add(1)
		go func(batch []model.SymbolTick) error {
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
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsBookTicksStorage) sendMicroBatch(ctx context.Context, bookTicks []model.SymbolTick) error {
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

package cs

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"

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
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, update_id, ask_price, ask_count, bid_price, bid_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
}

func (s CsBookTicksStorage) SendBookTicks(ctx context.Context, bookTicks []model.SymbolTick) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, bookTick := range bookTicks {
		batch.Query(s.insertStatement, bookTick.Symbol, getHourNo(bookTick.Timestamp), bookTick.Timestamp, bookTick.UpdateId, bookTick.AskPrice, bookTick.AskQuantity, bookTick.BidPrice, bookTick.BidQuantity)
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

package repo

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/pkg/clickhouse"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
)

type ChDeltaStorage struct {
	logger    *zap.Logger
	pool      *clickhouse.ChPoolHolder
	dbName    string
	tableName string
}

const ChDateTimeLayout = "2006-01-02 15:04:05.999999999"

const (
	TimestampCol     = "timestamp"
	DeltaTypeCol     = "delta_type"
	PriceCol         = "price"
	CountCol         = "count"
	UpdateIdCol      = "update_id"
	FirstUpdateIdCol = "first_update_id"
	SymbolCol        = "symbol"
)

func NewChDeltaStorage(pool *clickhouse.ChPoolHolder, dbName, tableName string) *ChDeltaStorage {
	return &ChDeltaStorage{
		logger:    log.GetLogger("ChDeltaStorage"),
		pool:      pool,
		dbName:    dbName,
		tableName: tableName,
	}
}

func prepareDeltasInsertBlock(deltas []model.Delta) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var updateIdCol proto.ColInt64
	var firstUpdateIdCol proto.ColInt64
	var symbCol proto.ColStr
	for _, delta := range deltas {
		timestampCol.Append(time.UnixMilli(delta.Timestamp))
		if delta.T {
			typeCol.Append("bid")
		} else {
			typeCol.Append("ask")
		}
		priceCol.Append(delta.Price)
		countCol.Append(delta.Count)
		updateIdCol.Append(delta.UpdateId)
		firstUpdateIdCol.Append(delta.FirstUpdateId)
		symbCol.Append(string(delta.Symbol))
	}
	return proto.Input{
		{Name: SymbolCol, Data: &symbCol},
		{Name: DeltaTypeCol, Data: &typeCol},
		{Name: TimestampCol, Data: timestampCol},
		{Name: PriceCol, Data: &priceCol},
		{Name: CountCol, Data: &countCol},
		{Name: UpdateIdCol, Data: &updateIdCol},
		{Name: FirstUpdateIdCol, Data: &firstUpdateIdCol},
	}
}

func (s ChDeltaStorage) SendDeltas(ctx context.Context, deltas []model.Delta) error {
	if s.pool == nil {
		return clickhouse.NilConnPool
	}
	if len(deltas) == 0 {
		s.logger.Warn("empty deltas batch")
		return nil
	}
	input := prepareDeltasInsertBlock(deltas)
	insertQuery := fmt.Sprintf("INSERT INTO %s.%s VALUES", s.dbName, s.tableName)
	err := s.pool.Do(ctx, ch.Query{
		Body:  insertQuery,
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending deltas %w", err)
	}
	return nil
}

func formListOfSymbolsForChQuery(symbols map[string]struct{}) string {
	if len(symbols) == 0 {
		return "()"
	}
	result := "("
	for symbol, _ := range symbols {
		result += fmt.Sprintf("'%s', ", symbol)
	}
	return result[:len(result)-2] + ")"
}

func (s ChDeltaStorage) GetTsSegment(ctx context.Context, since time.Time) (map[string]svc.TimePair, error) {
	earliestColName := "earliestTs"
	latestColName := "latestTs"
	earliestTsCol := new(proto.ColDateTime64).WithPrecision(3)
	latestTsCol := new(proto.ColDateTime64).WithPrecision(3)
	var symbCol proto.ColStr
	symbToTimePair := make(map[string]svc.TimePair)
	selectQueryBody := fmt.Sprintf("SELECT MIN(%s) AS %s, MAX(%s) AS %s, %s FROM %s.%s WHERE %s >= '%s' GROUP BY %s",
		TimestampCol, earliestColName, TimestampCol, latestColName, SymbolCol, s.dbName, s.tableName, TimestampCol, since.Format(ChDateTimeLayout), SymbolCol)
	s.logger.Info(selectQueryBody)
	selectQuery := ch.Query{
		Body: selectQueryBody,
		Result: proto.Results{
			{Name: earliestColName, Data: earliestTsCol},
			{Name: latestColName, Data: latestTsCol},
			{Name: SymbolCol, Data: &symbCol},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				symbToTimePair[symbCol.Row(i)] = svc.TimePair{
					Earliest: earliestTsCol.Row(i),
					Latest:   latestTsCol.Row(i),
				}
				s.logger.Debug(fmt.Sprintf("symbol %s presented from %s to %s", symbCol.Row(i), earliestTsCol.Row(i), latestTsCol.Row(i)))
			}
			return nil
		},
	}
	if err := s.pool.Do(ctx, selectQuery); err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return symbToTimePair, nil
}

func (s ChDeltaStorage) formGetDeltasQueryBody(symbol, deltaType string, fromTime, toTime time.Time) string {
	fromDateTime := fromTime.Format(ChDateTimeLayout)
	toDateTime := toTime.Format(ChDateTimeLayout)
	if deltaType == "" {
		return fmt.Sprintf(
			"SELECT * FROM %s.%s WHERE %s = '%s' AND %s BETWEEN '%s' AND '%s'",
			s.dbName, s.tableName, SymbolCol, symbol, TimestampCol, fromDateTime, toDateTime)
	}
	return fmt.Sprintf(
		"SELECT * FROM %s.%s WHERE %s = '%s' AND %s BETWEEN '%s' AND '%s' AND %s = '%s'",
		s.dbName, s.tableName, SymbolCol, symbol, TimestampCol, fromDateTime, toDateTime, DeltaTypeCol, deltaType)
}

func (s ChDeltaStorage) GetDeltas(ctx context.Context, symbol, deltaType string, fromTime, toTime time.Time) ([]model.Delta, error) {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var updateIdCol proto.ColInt64
	var firstUpdateIdCol proto.ColInt64
	var symbCol proto.ColStr
	var receivedDeltas []model.Delta
	selectQueryBody := s.formGetDeltasQueryBody(symbol, deltaType, fromTime, toTime)
	s.logger.Info(selectQueryBody)
	selectQuery := ch.Query{
		Body: selectQueryBody,
		Result: proto.Results{
			{Name: SymbolCol, Data: &symbCol},
			{Name: DeltaTypeCol, Data: &typeCol},
			{Name: TimestampCol, Data: timestampCol},
			{Name: PriceCol, Data: &priceCol},
			{Name: CountCol, Data: &countCol},
			{Name: UpdateIdCol, Data: &updateIdCol},
			{Name: FirstUpdateIdCol, Data: &firstUpdateIdCol},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				var isBid bool
				if typeCol.Row(i) == "bid" {
					isBid = true
				} else {
					isBid = false
				}
				receivedDeltas = append(receivedDeltas, model.Delta{
					Timestamp:     timestampCol.Row(i).UnixMilli(),
					Price:         priceCol.Row(i),
					Count:         countCol.Row(i),
					FirstUpdateId: firstUpdateIdCol.Row(i),
					UpdateId:      updateIdCol.Row(i),
					T:             isBid,
					Symbol:        symbol,
				})
			}
			return nil
		},
	}
	if err := s.pool.Do(ctx, selectQuery); err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return receivedDeltas, nil
}

func (s ChDeltaStorage) DeleteDeltas(ctx context.Context, symbol string, fromTime, toTime time.Time) error {
	fromDateTime := fromTime.Format(ChDateTimeLayout)
	toDateTime := toTime.Format(ChDateTimeLayout)
	deleteQueryBody := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE %s BETWEEN '%s' AND '%s' AND %s = '%s'",
		s.dbName, s.tableName, TimestampCol, fromDateTime, toDateTime, SymbolCol, symbol)
	s.logger.Debug(deleteQueryBody)
	deleteQuery := ch.Query{
		Body: deleteQueryBody,
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.pool.Do(ctxWithTimeout, deleteQuery); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s ChDeltaStorage) Connect(ctx context.Context) error {
	return s.pool.Connect(ctx)
}

func (s ChDeltaStorage) Reconnect(ctx context.Context) error {
	return s.pool.Reconnect(ctx, 3)
}

func (s ChDeltaStorage) Disconnect(ctx context.Context) {
	s.pool.Disconnect()
}

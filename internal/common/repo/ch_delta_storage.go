package repo

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/pkg/clickhouse"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
	"time"
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
		{Name: TimestampCol, Data: timestampCol},
		{Name: DeltaTypeCol, Data: &typeCol},
		{Name: PriceCol, Data: &priceCol},
		{Name: CountCol, Data: &countCol},
		{Name: UpdateIdCol, Data: &updateIdCol},
		{Name: FirstUpdateIdCol, Data: &firstUpdateIdCol},
		{Name: SymbolCol, Data: &symbCol},
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

func (s ChDeltaStorage) formGetSymbolQueryBody(fromDateTime, toDateTime string, excludedSymbols map[string]struct{}) string {
	if len(excludedSymbols) == 0 {
		return fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s BETWEEN '%s' AND '%s' LIMIT 1",
			SymbolCol, s.dbName, s.tableName, TimestampCol, fromDateTime, toDateTime)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE NOT %s IN %s AND %s BETWEEN '%s' AND '%s' LIMIT 1",
		SymbolCol, s.dbName, s.tableName, SymbolCol, formListOfSymbolsForChQuery(excludedSymbols), TimestampCol, fromDateTime, toDateTime)
}

func (s ChDeltaStorage) GetEarliestTs(ctx context.Context) (time.Time, error) {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	selectQueryBody := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s LIMIT 1",
		TimestampCol, s.dbName, s.tableName, TimestampCol)
	selectQuery := ch.Query{
		Body: selectQueryBody,
		Result: proto.Results{
			{Name: TimestampCol, Data: timestampCol},
		},
	}
	if err := s.pool.Do(ctx, selectQuery); err != nil {
		s.logger.Error(err.Error())
		return time.Time{}, err
	}
	if timestampCol.Rows() == 0 {
		return time.Time{}, svc.EmptyStorage
	}
	return timestampCol.Row(0), nil
}

func (s ChDeltaStorage) GetSymbol(ctx context.Context, fromTsS, toTsS int64, excludedSymbols map[string]struct{}) (string, error) {
	fromDateTime := time.Unix(fromTsS, 0).Format(ChDateTimeLayout)
	toDateTime := time.Unix(toTsS, 0).Format(ChDateTimeLayout)
	var symbCol proto.ColStr
	selectQueryBody := s.formGetSymbolQueryBody(fromDateTime, toDateTime, excludedSymbols)
	selectQuery := ch.Query{
		Body: selectQueryBody,
		Result: proto.Results{
			{Name: SymbolCol, Data: &symbCol},
		},
	}
	if err := s.pool.Do(ctx, selectQuery); err != nil {
		s.logger.Error(err.Error())
		return "", err
	}
	if symbCol.Rows() == 0 {
		return "", nil
	}
	return symbCol.Row(0), nil
}

func (s ChDeltaStorage) GetDeltas(ctx context.Context, symbol string, fromTsS, toTsS int64) ([]model.Delta, error) {
	fromDateTime := time.Unix(fromTsS, 0).Format(ChDateTimeLayout)
	toDateTime := time.Unix(toTsS, 0).Format(ChDateTimeLayout)
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var updateIdCol proto.ColInt64
	var firstUpdateIdCol proto.ColInt64
	var symbCol proto.ColStr
	var receivedDeltas []model.Delta
	selectQueryBody := fmt.Sprintf(
		"SELECT * FROM %s.%s WHERE %s == %s AND %s BETWEEN '%s' AND '%s'",
		s.dbName, s.tableName, SymbolCol, symbol, TimestampCol, fromDateTime, toDateTime)
	selectQuery := ch.Query{
		Body: selectQueryBody,
		Result: proto.Results{
			{Name: TimestampCol, Data: timestampCol},
			{Name: DeltaTypeCol, Data: &typeCol},
			{Name: PriceCol, Data: &priceCol},
			{Name: CountCol, Data: &countCol},
			{Name: FirstUpdateIdCol, Data: &firstUpdateIdCol},
			{Name: UpdateIdCol, Data: &updateIdCol},
			{Name: SymbolCol, Data: &symbCol},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			fmt.Println(block.Rows)
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

func (s ChDeltaStorage) DeleteDeltas(ctx context.Context, symbol string, fromTsS, toTsS int64, deltaType string) error {
	fromDateTime := time.Unix(fromTsS, 0).Format(ChDateTimeLayout)
	toDateTime := time.Unix(toTsS, 0).Format(ChDateTimeLayout)
	deleteQueryBody := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE %s BETWEEN '%s' AND '%s' AND %s = %s AND %s = %s",
		s.dbName, s.tableName, TimestampCol, fromDateTime, toDateTime, SymbolCol, symbol, DeltaTypeCol, deltaType)
	deleteQuery := ch.Query{
		Body: deleteQueryBody,
	}
	if err := s.pool.Do(ctx, deleteQuery); err != nil {
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

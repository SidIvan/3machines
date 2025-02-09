package cs

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"fmt"

	"github.com/gocql/gocql"
)

type DeltaInsertQueryBuilder struct {
	insertStatement string
}

func NewDeltaInsertQueryBuilder(tableName string) *DeltaInsertQueryBuilder {
	return &DeltaInsertQueryBuilder{
		insertStatement: fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, first_update_id, update_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", tableName),
	}
}

func (s DeltaInsertQueryBuilder) BuildQuery(batch *gocql.Batch, key model.ProcessingKey, delta model.Delta) {
	batch.Query(s.insertStatement, key.Symbol, key.HourNo, delta.Timestamp, delta.T, delta.Price, delta.Count, delta.FirstUpdateId, delta.UpdateId)
}

type SnapshotInsertQueryBuilder struct {
	insertStatement string
}

func NewSnapshotInsertQueryBuilder(tableName string) *SnapshotInsertQueryBuilder {
	return &SnapshotInsertQueryBuilder{
		insertStatement: fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, last_update_id) VALUES (?, ?, ?, ?, ?, ?, ?)", tableName),
	}
}

func (s SnapshotInsertQueryBuilder) BuildQuery(batch *gocql.Batch, key model.ProcessingKey, snapshotPart model.DepthSnapshotPart) {
	batch.Query(s.insertStatement, key.Symbol, key.HourNo, snapshotPart.Timestamp, snapshotPart.T, snapshotPart.Price, snapshotPart.Count, snapshotPart.LastUpdateId)
}

type BookTicksInsertQueryBuilder struct {
	insertStatement string
}

func NewBookTicksInsertQueryBuilder(tableName string) *BookTicksInsertQueryBuilder {
	return &BookTicksInsertQueryBuilder{
		insertStatement: fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, update_id, ask_price, ask_quantity, bid_price, bid_quantity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", tableName),
	}
}

func (s BookTicksInsertQueryBuilder) BuildQuery(batch *gocql.Batch, key model.ProcessingKey, bookTick bmodel.SymbolTick) {
	batch.Query(s.insertStatement, bookTick.Symbol, GetHourNo(bookTick.Timestamp), bookTick.Timestamp, bookTick.UpdateId, bookTick.AskPrice, bookTick.AskQuantity, bookTick.BidPrice, bookTick.BidQuantity)
}

type KeyInsertQueryBuilder struct {
	insertStatement string
}

func NewKeyInsertQueryBuilder(tableName string) *KeyInsertQueryBuilder {
	return &KeyInsertQueryBuilder{
		insertStatement: fmt.Sprintf("INSERT INTO %s (symbol, hour) VALUES (?, ?)", tableName),
	}
}

func (s KeyInsertQueryBuilder) BuildQuery(batch *gocql.Batch, key model.ProcessingKey) {
	batch.Query(s.insertStatement, key.Symbol, key.HourNo)
}

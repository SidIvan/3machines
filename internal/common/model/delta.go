package model

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Delta struct {
	Timestamp     int64  `json:"timestamp" bson:"timestamp" parquet:"timestampMs"`
	Price         string `json:"price" bson:"price" parquet:"price"`
	Count         string `json:"count" bson:"count" parquet:"count"`
	UpdateId      int64  `json:"updateId" bson:"updateId" parquet:"updateId"`
	FirstUpdateId int64  `json:"firstUpdateId" bson:"firstUpdateId" parquet:"firstUpdateId"`
	T             bool   `json:"type" bson:"type" parquet:"isBid"`
	Symbol        string `json:"symbol" bson:"symbol" parquet:"symbol"`
}

type DeltaWithId struct {
	Timestamp     int64              `bson:"timestamp"`
	Price         string             `bson:"price"`
	Count         string             `bson:"count"`
	UpdateId      int64              `bson:"updateId"`
	FirstUpdateId int64              `bson:"firstUpdateId"`
	T             bool               `bson:"type"`
	Symbol        string             `bson:"symbol"`
	Id            primitive.ObjectID `bson:"_id"`
}

func (s DeltaWithId) MongoId() primitive.ObjectID {
	return s.Id
}

func (s DeltaWithId) ToData() Delta {
	return Delta{
		Timestamp:     s.Timestamp,
		Price:         s.Price,
		Count:         s.Count,
		UpdateId:      s.UpdateId,
		FirstUpdateId: s.FirstUpdateId,
		T:             s.T,
		Symbol:        s.Symbol,
	}
}

func (s *DeltaWithId) GetDelta() Delta {
	return NewDelta(s.Timestamp, s.Price, s.Count, s.UpdateId, s.FirstUpdateId, s.T, s.Symbol)
}

func NewDelta(timestamp int64, price, count string, updateId, firstUpdateId int64, t bool, symbol string) Delta {
	return Delta{
		Timestamp:     timestamp,
		Price:         price,
		Count:         count,
		UpdateId:      updateId,
		FirstUpdateId: firstUpdateId,
		T:             t,
		Symbol:        symbol,
	}
}

func (s *Delta) IsBid() bool {
	return s.T
}

func (s *Delta) IsAsk() bool {
	return !s.T
}

func (s *Delta) GetStringDeltaType() string {
	return StringOfDeltaType(s.T)
}

func StringOfDeltaType(isBid bool) string {
	if isBid {
		return "bid"
	}
	return "ask"
}

func (s *Delta) String() string {
	stringVal, _ := json.Marshal(s)
	return string(stringVal)
}

func (s Delta) GetTimestampMs() int64 {
	return s.Timestamp
}

func (s Delta) GetSymbol() string {
	return s.Symbol
}

package model

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Delta struct {
	Timestamp     int64  `json:"timestamp" bson:"timestamp" parquet:"timestampMs"`
	Price         string `json:"price" bson:"price" parquet:"price"`
	Count         string `json:"count" bson:"count" parquet:"count"`
	UpdateId      int64  `json:"updateId" bson:"updateId"`
	FirstUpdateId int64  `json:"firstUpdateId" bson:"firstUpdateId"`
	T             bool   `json:"type" bson:"type" parquet:"type"`
	Symbol        string `json:"symbol" bson:"symbol"`
}

type DeltaWithId struct {
	Timestamp     int64              `bson:"timestamp"`
	Price         string             `bson:"price"`
	Count         string             `bson:"count"`
	UpdateId      int64              `bson:"updateId"`
	FirstUpdateId int64              `bson:"firstUpdateId"`
	T             bool               `bson:"type"`
	Symbol        string             `bson:"symbol"`
	MongoId       primitive.ObjectID `bson:"_id"`
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

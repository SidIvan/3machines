package model

import (
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Delta struct {
	Timestamp     int64  `json:"timestamp" bson:"timestamp" parquet:"name=timestamp_ms, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price         string `json:"price" bson:"price" parquet:"name=price, type=BYTE_ARRAY, convertedtype=UTF8"`
	Count         string `json:"count" bson:"count" parquet:"name=count, type=BYTE_ARRAY, convertedtype=UTF8"`
	UpdateId      int64  `json:"updateId" bson:"updateId"`
	FirstUpdateId int64  `json:"firstUpdateId" bson:"firstUpdateId"`
	T             bool   `json:"type" bson:"type"`
	Symbol        string `json:"symbol" bson:"symbol"`
}

type DeltaWithId struct {
	Delta
	MongoId primitive.ObjectID `bson:"_id"`
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

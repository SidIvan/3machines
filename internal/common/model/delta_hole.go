package model

import "DeltaReceiver/pkg/binance/model"

type DeltaHole struct {
	Symbol        string `json:"symbol" bson:"symbol"`
	FirstUpdateId int64  `json:"first_update_id" bson:"first_update_id"`
	LastUpdateId  int64  `json:"last_update_id" bson:"last_update_id"`
	TimestampMs   int64  `json:"timestamp_ms" bson:"timestamp_ms"`
	MarketType    string `json:"market_type" bson:"market_type"`
}

func NewDeltaHole(symbol string, firstUpdateId, lastUpdateId, timestampMs int64, marketType model.DataType) DeltaHole {
	return DeltaHole{
		Symbol:        symbol,
		FirstUpdateId: firstUpdateId,
		LastUpdateId:  lastUpdateId,
		TimestampMs:   timestampMs,
		MarketType:    string(marketType),
	}
}

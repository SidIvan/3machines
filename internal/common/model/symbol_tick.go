package model

import (
	bmodel "DeltaReceiver/pkg/binance/model"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SymbolTickWithMongoId struct {
	UpdateId    int64              `json:"u" bson:"update_id" parquet:"updateId"`
	Symbol      string             `json:"s" bson:"symbol" parquet:"symbol"`
	BidPrice    string             `json:"b" bson:"bid_price" parquet:"bidPrice"`
	BidQuantity string             `json:"B" bson:"bid_quantity" parquet:"bidQuantity"`
	AskPrice    string             `json:"a" bson:"ask_price" parquet:"askPrice"`
	AskQuantity string             `json:"A" bson:"ask_quantity" parquet:"askQuantity"`
	Timestamp   int64              `bson:"timestamp_ms" parquet:"timestampMs"`
	Id          primitive.ObjectID `bson:"_id"`
}

func (s SymbolTickWithMongoId) MongoId() primitive.ObjectID {
	return s.Id
}

func (s SymbolTickWithMongoId) ToData() bmodel.SymbolTick {
	return bmodel.SymbolTick{
		UpdateId:    s.UpdateId,
		Symbol:      s.Symbol,
		BidPrice:    s.BidPrice,
		BidQuantity: s.BidQuantity,
		AskPrice:    s.AskPrice,
		AskQuantity: s.AskQuantity,
		Timestamp:   s.Timestamp,
	}
}

package model

import "encoding/json"

type SymbolTick struct {
	UpdateId    int64  `json:"u" bson:"update_id" parquet:"updateId"`
	Symbol      string `json:"s" bson:"symbol" parquet:"symbol"`
	BidPrice    string `json:"b" bson:"bid_price" parquet:"bidPrice"`
	BidQuantity string `json:"B" bson:"bid_quantity" parquet:"bidQuantity"`
	AskPrice    string `json:"a" bson:"ask_price" parquet:"askPrice"`
	AskQuantity string `json:"A" bson:"ask_quantity" parquet:"askQuantity"`
	Timestamp   int64  `bson:"timestamp_ms" parquet:"timestampMs"`
}

func (s *SymbolTick) String() string {
	stringVal, _ := json.Marshal(s)
	return string(stringVal)
}

func (s SymbolTick) GetTimestampMs() int64 {
	return s.Timestamp
}

func (s SymbolTick) GetSymbol() string {
	return s.Symbol
}

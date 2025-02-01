package model

import "encoding/json"

type SymbolTick struct {
	UpdateId    int64  `json:"u" bson:"update_id"`
	Symbol      string `json:"s" bson:"symbol"`
	BidPrice    string `json:"b" bson:"bid_price"`
	BidQuantity string `json:"B" bson:"bid_quantity"`
	AskPrice    string `json:"a" bson:"ask_price"`
	AskQuantity string `json:"A" bson:"ask_quantity"`
	Timestamp   int64  `bson:"timestamp_ms"`
}

func (s *SymbolTick) String() string {
	stringVal, _ := json.Marshal(s)
	return string(stringVal)
}

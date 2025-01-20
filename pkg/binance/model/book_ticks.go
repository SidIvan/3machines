package model

type SymbolTick struct {
	UpdateId    int64  `json:"u" bson:"update_id"`
	Symbol      string `json:"s" bson:"symbol"`
	BidPrice    string `json:"b" bson:"bid_price"`
	BidQuantity string `json:"B" bson:"bid_quantity"`
	AskPrice    string `json:"a" bson:"ask_price"`
	AskQuantity string `json:"A" bson:"ask_quantity"`
	Timestamp   int64  `bson:"timestamp_ms"`
}

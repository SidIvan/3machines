package model

type SymbolTick struct {
	Symbol      string `json:"symbol" bson:"symbol"`
	BidPrice    string `json:"bidPrice" bson:"bid_price"`
	BidQuantity string `json:"bidQty" bson:"bid_quantity"`
	AskPrice    string `json:"askPrice" bson:"ask_price"`
	AskQuantity string `json:"askQty" bson:"ask_quantity"`
}

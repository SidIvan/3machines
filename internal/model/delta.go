package model

type Delta struct {
	Timestamp     int64  `json:"timestamp" bson:"timestamp"`
	Price         string `json:"price" bson:"price"`
	Count         string `json:"count" bson:"count"`
	UpdateId      int64  `json:"updateId" bson:"updateId"`
	FirstUpdateId int64  `json:"firstUpdateId" bson:"firstUpdateId"`
	T             bool   `json:"type" bson:"type"`
	Symbol        Symbol `json:"symbol" bson:"symbol"`
}

func NewDelta(timestamp int64, price, count string, updateId, firstUpdateId int64, t bool, symbol Symbol) Delta {
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

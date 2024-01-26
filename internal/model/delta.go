package model

type Delta struct {
	Timestamp int64   `json:"timestamp" bson:"timestamp"`
	Price     float64 `json:"price" bson:"price"`
	Count     float64 `json:"count" bson:"count"`
	UpdateId  int64   `json:"updateId" bson:"updateId"`
	T         bool    `json:"type" bson:"type"`
	Symbol    Symbol  `json:"symbol" bson:"symbol"`
}

func NewDelta(timestamp int64, price, count float64, updateId int64, t bool, symbol Symbol) Delta {
	return Delta{
		Timestamp: timestamp,
		Price:     price,
		Count:     count,
		UpdateId:  updateId,
		T:         t,
		Symbol:    symbol,
	}
}

func (s *Delta) IsBid() bool {
	return s.T
}

func (s *Delta) IsAsk() bool {
	return !s.T
}

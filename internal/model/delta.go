package model

type Delta struct {
	Timestamp int64   `json:"timestamp" bson:"timestamp"`
	Price     float64 `json:"price" bson:"price"`
	Count     float64 `json:"count" bson:"count"`
	UpdateId  int64   `json:"updateId" bson:"updateId"`
	T         bool    `json:"type" bson:"type"`
}

func (s *Delta) IsBid() bool {
	return s.T
}

func (s *Delta) IsAsk() bool {
	return !s.T
}

package model

type Delta struct {
	Timestamp int64   `json:"timestamp"`
	Price     float64 `json:"price"`
	Count     float64 `json:"count"`
	T         bool    `json:"type"`
}

func (s *Delta) IsBid() bool {
	return s.T
}

func (s *Delta) IsAsk() bool {
	return !s.T
}

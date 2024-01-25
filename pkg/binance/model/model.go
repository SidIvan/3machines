package model

type DeltaMessage struct {
	EventType     string      `json:"e"`
	EventTime     int64       `json:"E"`
	Symbol        string      `json:"s"`
	UpdateId      int64       `json:"u"`
	FirstUpdateId int64       `json:"U"`
	Bids          [][2]string `json:"b"`
	Asks          [][2]string `json:"a"`
}

type DepthSnapshot struct {
	LastUpdateId int64       `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

type Symbol string

const (
	BTCUSDT = Symbol("BTCUSDT")
)

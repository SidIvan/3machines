package model

import "fmt"

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

type DataType string

const (
	Spot        DataType = "spot"
	FuturesUSD  DataType = "usd"
	FuturesCoin DataType = "coin"
)

func (s DataType) ExInfoQuery() string {
	if s == Spot {
		return "/api/v3/exchangeInfo"
	} else if s == FuturesUSD {
		return "/fapi/v1/exchangeInfo"
	} else if s == FuturesCoin {
		return "/dapi/v1/exchangeInfo"
	}
	panic(fmt.Sprintf("unexpected DataType %s", s))
}

func (s DataType) DepthSnapshotQuery() string {
	if s == Spot {
		return "/api/v3/depth"
	} else if s == FuturesUSD {
		return "/fapi/v1/depth"
	} else if s == FuturesCoin {
		return "/dapi/v1/depth"
	}
	panic(fmt.Sprintf("unexpected DataType %s", s))
}

package model

type DepthSnapshotPart struct {
	LastUpdateId int64  `json:"last_update_id" bson:"last_update_id" parquet:"lastUpdateId"`
	T            bool   `json:"is_bid" bson:"is_bid" parquet:"isBid"`
	Price        string `json:"price" bson:"price" parquet:"price"`
	Count        string `json:"count" bson:"count" parquet:"count"`
	Symbol       string `json:"symbol" bson:"symbol" parquet:"symbol"`
	Timestamp    int64  `json:"timestamp" bson:"timestamp" parquet:"timestampMs"`
}

func NewDepthSnapshotPart(lastUpdateId int64, t bool, price, count string, symb string, timestamp int64) DepthSnapshotPart {
	return DepthSnapshotPart{
		LastUpdateId: lastUpdateId,
		T:            t,
		Price:        price,
		Count:        count,
		Symbol:       symb,
		Timestamp:    timestamp,
	}
}

func (s *DepthSnapshotPart) isBid() bool {
	return s.T
}

func (s *DepthSnapshotPart) isAsk() bool {
	return !s.T
}

func (s DepthSnapshotPart) GetTimestampMs() int64 {
	return s.Timestamp
}

func (s DepthSnapshotPart) GetSymbol() string {
	return s.Symbol
}

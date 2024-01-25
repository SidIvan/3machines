package model

type DepthSnapshotPart struct {
	LastUpdateId int64   `json:"last_update_id" bson:"last_update_id"`
	T            bool    `json:"is_bid" bson:"is_bid"`
	Price        float64 `json:"price" bson:"price"`
	Count        float64 `json:"count" bson:"count"`
}

func (s *DepthSnapshotPart) isBid() bool {
	return s.T
}

func (s *DepthSnapshotPart) isAsk() bool {
	return !s.T
}

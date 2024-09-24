package model

import cmodel "DeltaReceiver/internal/common/model"

type DeltaHoleWithInfo struct {
	ServiceName   string `json:"service_name" bson:"service_name"`
	Symbol        string `json:"symbol" bson:"symbol"`
	T             string `json:"type" bson:"type"`
	FirstUpdateId int64  `json:"first_update_id" bson:"first_update_id"`
	LastUpdateId  int64  `json:"last_update_id" bson:"last_update_id"`
	TimestampMs   int64  `json:"timestamp_ms" bson:"timestamp_ms"`
}

func NewDeltaHoleWithInfo(serviceName string, deltaHole *cmodel.DeltaHole) *DeltaHoleWithInfo {
	return &DeltaHoleWithInfo{
		ServiceName:   serviceName,
		Symbol:        deltaHole.Symbol,
		T:             deltaHole.T,
		FirstUpdateId: deltaHole.FirstUpdateId,
		LastUpdateId:  deltaHole.LastUpdateId,
		TimestampMs:   deltaHole.TimestampMs,
	}
}

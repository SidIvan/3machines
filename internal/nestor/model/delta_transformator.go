package model

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
)

type DeltaDataTransformator struct {
}

func NewDeltaDataTransformator() *DeltaDataTransformator {
	return &DeltaDataTransformator{}
}

func (s DeltaDataTransformator) Transform(msg bmodel.DeltaMessage) ([]model.Delta, error) {
	var batch []model.Delta
	for _, bid := range msg.Bids {
		batch = append(batch, model.NewDelta(msg.EventTime, bid[0], bid[1], msg.UpdateId, msg.FirstUpdateId, true, msg.Symbol))
	}
	for _, ask := range msg.Asks {
		batch = append(batch, model.NewDelta(msg.EventTime, ask[0], ask[1], msg.UpdateId, msg.FirstUpdateId, false, msg.Symbol))
	}
	return batch, nil
}

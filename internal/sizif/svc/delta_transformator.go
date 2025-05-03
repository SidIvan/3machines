package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/web"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"fmt"
	"sort"

	"go.uber.org/zap"
)

type DeltaTransformator struct {
	logger      *zap.Logger
	dwarfClient *web.DwarfHttpClient
	marketType  bmodel.DataType
}

func NewDeltaTransformator(dwarfClient *web.DwarfHttpClient, marketType bmodel.DataType) *DeltaTransformator {
	return &DeltaTransformator{
		logger:      log.GetLogger("DeltaTransformator"),
		dwarfClient: dwarfClient,
		marketType:  marketType,
	}
}

const millisInHour = 60 * 60 * 1000

func (s DeltaTransformator) Transform(deltas []model.Delta, key *model.ProcessingKey) ([][]model.Delta, bool) {
	if len(deltas) == 0 {
		s.logger.Warn(fmt.Sprintf("empty batch for key %s", key))
		return nil, false
	}
	minAllowedTsMs := key.HourNo * millisInHour
	maxAllowedTsMs := minAllowedTsMs + millisInHour - 1
	deltasOutsideTimeRange := 0
	for _, delta := range deltas {
		if delta.Timestamp > maxAllowedTsMs || delta.Timestamp < minAllowedTsMs {
			s.logger.Debug(delta.String())
			deltasOutsideTimeRange++
		}
	}
	validByTimeRange := true
	if deltasOutsideTimeRange > 0 {
		validByTimeRange = false
	}
	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].UpdateId < deltas[j].UpdateId || (deltas[i].UpdateId == deltas[j].UpdateId && deltas[i].FirstUpdateId < deltas[j].FirstUpdateId)
	})
	deltaHoles := 0
	lastUpdateId := deltas[0].UpdateId
	for i := 1; i < len(deltas); i++ {
		if deltas[i].FirstUpdateId-lastUpdateId > 1 {
			// ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			// s.dwarfClient.SaveDeltaHole(ctx, model.NewDeltaHole(deltas[i].Symbol, lastUpdateId, deltas[i].FirstUpdateId, deltas[i].Timestamp, s.marketType))
			// cancel()
			deltaHoles++
		}
		lastUpdateId = deltas[i].UpdateId
	}
	validByHoles := true
	if deltaHoles > 0 {
		validByHoles = false
	}
	return [][]model.Delta{deltas}, validByHoles && validByTimeRange
}

package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"fmt"
	"sort"

	"go.uber.org/zap"
)

type DeltaTransformator struct {
	logger *zap.Logger
}

func NewDeltaTransformator() *DeltaTransformator {
	return &DeltaTransformator{
		logger: log.GetLogger("DeltaTransformator"),
	}
}

const millisInHour = 60 * 60 * 1000

func (s DeltaTransformator) Transform(deltas []model.Delta, key *model.ProcessingKey) ([]model.Delta, bool) {
	if len(deltas) == 0 {
		s.logger.Warn(fmt.Sprintf("empty batch for key %s", key))
		return deltas, false
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
			// s.logger.Info(fmt.Sprintf("hole %d %d", lastUpdateId, deltas[i].FirstUpdateId))
			deltaHoles++
		}
		lastUpdateId = deltas[i].UpdateId
	}
	validByHoles := true
	if deltaHoles > 0 {
		// s.logger.Warn(fmt.Sprintf("%d holes for key %s", deltaHoles, key))
		// data, _ := json.Marshal(deltas)
		// s.logger.Info("holed batch" + string(data))
		validByHoles = false
	}
	return deltas, validByHoles && validByTimeRange
}

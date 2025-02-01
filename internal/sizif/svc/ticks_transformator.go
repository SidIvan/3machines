package svc

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"fmt"
	"sort"

	"go.uber.org/zap"
)

type BookTicksTransformator struct {
	logger *zap.Logger
}

func NewBookTicksTransformator() *BookTicksTransformator {
	return &BookTicksTransformator{
		logger: log.GetLogger("BookTicksTransformator"),
	}
}

func (s BookTicksTransformator) Transform(ticks []bmodel.SymbolTick, key *model.ProcessingKey) ([]bmodel.SymbolTick, bool) {
	if len(ticks) == 0 {
		s.logger.Warn(fmt.Sprintf("empty batch for key %s", key))
		return ticks, false
	}
	minAllowedTsMs := key.HourNo * millisInHour
	maxAllowedTsMs := minAllowedTsMs + millisInHour - 1
	ticksOutsideTimeRange := 0
	for _, tick := range ticks {
		if tick.Timestamp > maxAllowedTsMs || tick.Timestamp < minAllowedTsMs {
			s.logger.Debug(tick.String())
			ticksOutsideTimeRange++
		}
	}
	validByTimeRange := true
	if ticksOutsideTimeRange > 0 {
		validByTimeRange = false
	}
	sort.Slice(ticks, func(i, j int) bool {
		return ticks[i].UpdateId < ticks[j].UpdateId
	})
	tickUpdateIds := make(map[int64]struct{})
	updateIdDuplicates := 0
	for i := 0; i < len(ticks); i++ {
		if _, ok := tickUpdateIds[ticks[i].UpdateId]; ok {
			updateIdDuplicates++
		} else {
			tickUpdateIds[ticks[i].UpdateId] = struct{}{}
		}
	}
	validByDuplicates := true
	if updateIdDuplicates > 0 {
		validByDuplicates = false
	}
	return ticks, validByDuplicates && validByTimeRange
}

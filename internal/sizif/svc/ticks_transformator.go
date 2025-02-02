package svc

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"encoding/json"
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

func (s BookTicksTransformator) Transform(ticks []bmodel.SymbolTick, key *model.ProcessingKey) ([][]bmodel.SymbolTick, bool) {
	if len(ticks) == 0 {
		s.logger.Warn(fmt.Sprintf("empty batch for key %s", key))
		return nil, false
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
		s.logger.Warn(fmt.Sprintf("invalid time range for %s key", key))
		validByTimeRange = false
	}
	sort.Slice(ticks, func(i, j int) bool {
		return ticks[i].UpdateId < ticks[j].UpdateId || (ticks[i].UpdateId == ticks[j].UpdateId && ticks[i].Timestamp < ticks[j].UpdateId)
	})
	tickUpdateIds := make(map[int64]struct{})
	var pqtTicks []bmodel.SymbolTick
	pqtTicks = append(pqtTicks, ticks[0])
	updateIdDuplicates := 0
	for i := 1; i < len(ticks); i++ {
		curTick := ticks[i]
		if _, ok := tickUpdateIds[curTick.UpdateId]; ok {
			prevTick := pqtTicks[len(pqtTicks)-1]
			if prevTick.AskPrice != curTick.AskPrice || prevTick.AskQuantity != curTick.AskQuantity || prevTick.BidPrice != curTick.BidPrice || prevTick.BidQuantity != curTick.BidQuantity {
				updateIdDuplicates++
				pqtTicks = append(pqtTicks, curTick)
			}
		} else {
			tickUpdateIds[ticks[i].UpdateId] = struct{}{}
			pqtTicks = append(pqtTicks, curTick)
		}
	}
	validByDuplicates := true
	if updateIdDuplicates > 0 {
		s.logger.Warn(fmt.Sprintf("invalid by duplicates for %s key", key))
		data, _ := json.Marshal(pqtTicks)
		s.logger.Info(fmt.Sprintf("ticks batch, %s", string(data)))
		validByDuplicates = false
	}
	return [][]bmodel.SymbolTick{ticks}, validByDuplicates && validByTimeRange
}

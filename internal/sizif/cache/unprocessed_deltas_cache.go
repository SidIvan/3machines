package cache

import (
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const updateUnprocessedDeltasCachePeriod = 20 * time.Minute

type UnprocessedDeltasCache struct {
	logger                 *zap.Logger
	mut                    *sync.Mutex
	deltaStorage           svc.DeltaStorage
	lastUpdateTs           time.Time
	cacheVal               map[string]svc.TimePair
	symbolToLastUpdateTime map[string]time.Time
}

func NewUnprocessedDeltasCache(deltaStorage svc.DeltaStorage) *UnprocessedDeltasCache {
	var mut sync.Mutex
	return &UnprocessedDeltasCache{
		logger:                 log.GetLogger("UnprocessedDeltasCache"),
		mut:                    &mut,
		deltaStorage:           deltaStorage,
		lastUpdateTs:           time.UnixMicro(0),
		cacheVal:               make(map[string]svc.TimePair),
		symbolToLastUpdateTime: make(map[string]time.Time),
	}
}

func trunkSinceStartTs(timePairs map[string]svc.TimePair, sinceTime time.Time) {
	for symbol, timePair := range timePairs {
		if timePair.Earliest.Before(sinceTime) {
			timePairs[symbol] = svc.TimePair{
				Earliest: sinceTime,
				Latest:   timePair.Latest,
			}
		}
	}
}

var location, _ = time.LoadLocation("Europe/Moscow")

func (s *UnprocessedDeltasCache) GetUnprocessedDeltas(ctx context.Context, since time.Time) (map[string]svc.TimePair, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	curTime := time.Now()
	if curTime.Sub(s.lastUpdateTs) > updateUnprocessedDeltasCachePeriod {
		s.logger.Debug("updating cache")
		newCacheVal, err := s.deltaStorage.GetTsSegment(ctx, since)
		if err != nil {
			s.logger.Error(err.Error())
			return s.cacheVal, err
		}

		for key, val := range s.symbolToLastUpdateTime {
			if curTimePair, ok := newCacheVal[key]; ok && curTimePair.Earliest.Before(val) {
				s.logger.Debug(fmt.Sprintf("update cache for symbol %s from %s to %s", key, curTimePair.Earliest, val))
				newCacheVal[key] = svc.TimePair{
					Earliest: val,
					Latest:   curTimePair.Latest,
				}
			}
		}
		s.cacheVal = newCacheVal
		s.lastUpdateTs = time.Now()
		return newCacheVal, nil
	}
	return s.cacheVal, nil
}

func (s *UnprocessedDeltasCache) SetProcessedSymbolTo(symbol string, toTime time.Time) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if oldVal, ok := s.symbolToLastUpdateTime[symbol]; !ok || oldVal.Before(toTime) {
		s.symbolToLastUpdateTime[symbol] = toTime
	}
}

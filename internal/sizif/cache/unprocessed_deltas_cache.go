package cache

import (
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"go.uber.org/zap"
	"sync"
	"time"
)

const updateUnprocessedDeltasCachePeriod = 5 * time.Minute

type UnprocessedDeltasCache struct {
	logger       *zap.Logger
	mut          *sync.Mutex
	deltaStorage svc.DeltaStorage
	lastUpdateTs time.Time
	cacheVal     map[string]svc.TimePair
}

func NewUnprocessedDeltasCache(deltaStorage svc.DeltaStorage) *UnprocessedDeltasCache {
	var mut sync.Mutex
	return &UnprocessedDeltasCache{
		logger:       log.GetLogger("UnprocessedDeltasCache"),
		mut:          &mut,
		deltaStorage: deltaStorage,
		lastUpdateTs: time.UnixMicro(0),
		cacheVal:     make(map[string]svc.TimePair),
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
		s.cacheVal = newCacheVal
		s.lastUpdateTs = curTime
		return newCacheVal, nil
	}
	return s.cacheVal, nil
}

package cache

import (
	"DeltaReceiver/internal/common/model"
	"sync"
)

type DeltaUpdateIdWatcher struct {
	val map[string]int64
	mut *sync.Mutex
}

func NewDeltaUpdateIdWatcher() *DeltaUpdateIdWatcher {
	var mut sync.Mutex
	return &DeltaUpdateIdWatcher{
		val: make(map[string]int64),
		mut: &mut,
	}
}

func (s *DeltaUpdateIdWatcher) GetHolesAndUpdate(batch []model.Delta) []model.DeltaHole {
	var holes []model.DeltaHole
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, delta := range batch {
		symbol := delta.Symbol
		var lastUpdId int64
		var ok bool
		if lastUpdId, ok = s.val[symbol]; ok {
			if delta.FirstUpdateId-lastUpdId > 1 {
				holes = append(holes, *model.NewDeltaHole(
					symbol,
					lastUpdId+1,
					delta.FirstUpdateId,
					delta.Timestamp,
				))
			}
		}
		s.val[symbol] = max(lastUpdId, delta.UpdateId)
	}
	return holes
}

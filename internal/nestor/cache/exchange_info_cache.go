package cache

import (
	"DeltaReceiver/pkg/binance/model"
	"sync"
)

type ExchangeInfoCache struct {
	val *model.ExchangeInfo
	mut *sync.Mutex
}

func NewExchangeInfoCache() *ExchangeInfoCache {
	var mut sync.Mutex
	return &ExchangeInfoCache{
		mut: &mut,
	}
}

func (s *ExchangeInfoCache) SetVal(val *model.ExchangeInfo) {
	s.mut.Lock()
	s.val = val
	s.mut.Unlock()
}

func (s *ExchangeInfoCache) GetVal() *model.ExchangeInfo {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.val
}

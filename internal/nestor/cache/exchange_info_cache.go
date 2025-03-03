package cache

import (
	"DeltaReceiver/internal/common/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"sync"
	"time"
)

type ExchangeInfoCache struct {
	val                        *model.ExchangeInfo
	tradingSymbols             []string
	mut                        *sync.Mutex
	requestWeightLimit         int
	requestWeightLimitDuration time.Duration
	sufOfLimitHeader           string
}

func NewExchangeInfoCache() *ExchangeInfoCache {
	var mut sync.Mutex
	return &ExchangeInfoCache{
		mut: &mut,
	}
}

func (s *ExchangeInfoCache) SetVal(val bmodel.ExInfo) {
	s.mut.Lock()
	s.val = model.NewExchangeInfo(val)
	s.tradingSymbols = val.GetTradingSymbols()
	s.requestWeightLimit = val.GetRequestWeightLimit()
	s.requestWeightLimitDuration = val.GetRequestWeightLimitDuration()
	s.sufOfLimitHeader = val.GetSuffixOfLimitHeader()
	s.mut.Unlock()
}

func (s *ExchangeInfoCache) GetVal() *model.ExchangeInfo {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.val
}

func (s *ExchangeInfoCache) GetTradingSymbols() []string {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.tradingSymbols
}

func (s *ExchangeInfoCache) GetRequestWeightLimit() int {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.requestWeightLimit
}

func (s *ExchangeInfoCache) GetRequestWeightLimitDuration() time.Duration {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.requestWeightLimitDuration
}

func (s *ExchangeInfoCache) GetSuffixOfLimitHeader() string {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.sufOfLimitHeader
}

package model

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

type ExchangeInfo struct {
	Timezone        string          `json:"timezone"`
	ServerTime      int64           `json:"serverTime"`
	RateLimits      []RateLimitInfo `json:"rateLimits"`
	ExchangeFilters json.RawMessage `json:"exchangeFilters"`
	Symbols         []SymbolInfo    `json:"symbols"`
}

type SymbolInfo struct {
	Symbol                          string          `json:"symbol"`
	Status                          string          `json:"status"`
	BaseAsset                       string          `json:"baseAsset"`
	BaseAssetPrediction             int             `json:"baseAssetPrecision"`
	QuoteAsset                      string          `json:"quoteAsset"`
	QuotePrecision                  int             `json:"quotePrecision"`
	QuoteAssetPrecision             int             `json:"quoteAssetPrecision"`
	OrderTypes                      []string        `json:"orderTypes"`
	IcebergAllowed                  bool            `json:"icebergAllowed"`
	OcoAllowed                      bool            `json:"ocoAllowed"`
	QuoteOrderQtyMarketAllowed      bool            `json:"quoteOrderQtyMarketAllowed"`
	AllowTrailingStop               bool            `json:"allowTrailingStop"`
	CancelReplaceAllowed            bool            `json:"cancelReplaceAllowed"`
	IsSpotTradingAllowed            bool            `json:"isSpotTradingAllowed"`
	IsMarginTradingAllowed          bool            `json:"isMarginTradingAllowed"`
	Filters                         json.RawMessage `json:"filters"`
	Permissions                     []string        `json:"permissions"`
	DefaultSelfTradePreventionMode  string          `json:"defaultSelfTradePreventionMode"`
	AllowedSelfTradePreventionModes []string        `json:"allowedSelfTradePreventionModes"`
}

type RateLimitInfo struct {
	Type        string `json:"rateLimitType"`
	Interval    string `json:"interval"`
	IntervalNum int    `json:"intervalNum"`
	Limit       int    `json:"limit"`
}

func ExInfoStringHash(s string) int64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64())
}

func (s *ExchangeInfo) ExInfoHash() int64 {
	tmp := *s
	tmp.ServerTime = 0
	payload, _ := json.Marshal(tmp)
	return ExInfoStringHash(string(payload))
}

func (s *ExchangeInfo) GetRequestWeightLimitDuration() time.Duration {
	for _, rateLimit := range s.RateLimits {
		if rateLimit.Type == "REQUEST_WEIGHT" {
			if rateLimit.Interval == "SECOND" {
				return time.Duration(rateLimit.IntervalNum) * time.Second
			} else if rateLimit.Interval == "MINUTE" {
				return time.Duration(rateLimit.IntervalNum) * time.Minute
			} else if rateLimit.Interval == "HOUR" {
				return time.Duration(rateLimit.IntervalNum) * time.Hour
			} else if rateLimit.Interval == "DAY" {
				return time.Duration(rateLimit.IntervalNum) * 24 * time.Hour
			}
		}
	}
	return 0
}

func (s *ExchangeInfo) GetRequestWeightLimit() int {
	for _, rateLimit := range s.RateLimits {
		if rateLimit.Type == "REQUEST_WEIGHT" {
			return rateLimit.Limit
		}
	}
	return 0
}

func (s *ExchangeInfo) GetSuffixOfLimitHeader() string {
	for _, rateLimit := range s.RateLimits {
		if rateLimit.Type == "REQUEST_WEIGHT" {
			return strings.ToLower(fmt.Sprintf("%d%c", rateLimit.IntervalNum, rateLimit.Interval[0]))
		}
	}
	return ""
}

package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type CoinExchangeInfo struct {
	ExchangeFilters json.RawMessage  `json:"exchangeFilters"`
	RateLimits      []RateLimitInfo  `json:"rateLimits"`
	ServerTime      int64            `json:"serverTime"`
	Timezone        string           `json:"timezone"`
	Symbols         []CoinSymbolInfo `json:"symbols"`
}

type CoinSymbolInfo struct {
	Filters               json.RawMessage `json:"filters"`
	OrderType             []string        `json:"OrderType"`
	TimeInForce           []string        `json:"timeInForce"`
	LiquidationFee        string          `json:"liquidationFee"`
	MarketTakeBound       string          `json:"marketTakeBound"`
	Symbol                string          `json:"symbol"`
	Pair                  string          `json:"pair"`
	ContractType          string          `json:"contractType"`
	DeliveryDate          int64           `json:"deliveryDate"`
	OnboardDate           int64           `json:"onboardDate"`
	ContractStatus        string          `json:"contractStatus"`
	ContractSize          int64           `json:"contractSize"`
	QuoteAsset            string          `json:"quoteAsset"`
	BaseAsset             string          `json:"baseAsset"`
	MarginAsset           string          `json:"marginAsset"`
	PricePrecision        int64           `json:"pricePrecision"`
	QuantityPrecision     int64           `json:"quantityPrecision"`
	BaseAssetPrecision    int64           `json:"baseAssetPrecision"`
	QuotePrecision        int64           `json:"quotePrecision"`
	EqualQtyPrecision     int64           `json:"equalQtyPrecision"`
	TriggerProtect        string          `json:"triggerProtect"`
	MaintMarginPercent    string          `json:"maintMarginPercent"`
	RequiredMarginPercent string          `json:"requiredMarginPercent"`
	UnderlyingType        string          `json:"underlyingType"`
	UnderlyingSubType     json.RawMessage `json:"underlyingSubType"`
}

func (s *CoinExchangeInfo) ExInfoHash() int64 {
	tmp := *s
	tmp.ServerTime = 0
	payload, _ := json.Marshal(tmp)
	return ExInfoStringHash(string(payload))
}

func (s *CoinExchangeInfo) GetRequestWeightLimitDuration() time.Duration {
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

func (s *CoinExchangeInfo) GetRequestWeightLimit() int {
	for _, rateLimit := range s.RateLimits {
		if rateLimit.Type == "REQUEST_WEIGHT" {
			return rateLimit.Limit
		}
	}
	return 0
}

func (s *CoinExchangeInfo) GetSuffixOfLimitHeader() string {
	for _, rateLimit := range s.RateLimits {
		if rateLimit.Type == "REQUEST_WEIGHT" {
			return strings.ToLower(fmt.Sprintf("%d%c", rateLimit.IntervalNum, rateLimit.Interval[0]))
		}
	}
	return ""
}

func (s *CoinExchangeInfo) ServerTimeMs() int64 {
	return s.ServerTime
}

func (s *CoinExchangeInfo) GetTradingSymbols() []string {
	var tradingSymbols []string
	for _, symbol := range s.Symbols {
		if symbol.ContractStatus == "TRADING" {
			tradingSymbols = append(tradingSymbols, symbol.Symbol)
		}
	}
	return tradingSymbols
}

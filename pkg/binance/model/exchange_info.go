package model

import (
	"encoding/json"
	"reflect"
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

func EqualsExchangeInfos(info1 *ExchangeInfo, info2 *ExchangeInfo) bool {
	return info1.Timezone == info2.Timezone &&
		reflect.DeepEqual(info1.ExchangeFilters, info2.ExchangeFilters) &&
		reflect.DeepEqual(info1.RateLimits, info2.RateLimits) &&
		reflect.DeepEqual(info1.Symbols, info2.Symbols)
}

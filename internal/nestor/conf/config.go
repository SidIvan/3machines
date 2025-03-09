package conf

import (
	"DeltaReceiver/internal/common/conf"
	"os"
	"strconv"
)

type AppConfig struct {
	Mode             BinanceMode        `yaml:"binance.mode"`
	ReconnectPeriodM int16              `yaml:"binance.reconnect.period.m"`
	MongoRepoCfg     *MongoRepoConfig   `yaml:"mongo"`
	CsCfg            *conf.CsRepoConfig `yaml:"socrates"`
	BinanceSpotCfg   *BinanceMarketCfg  `yaml:"binance.spot"`
	BinanceUSDCfg    *BinanceMarketCfg  `yaml:"binance.usd"`
	BinanceCoinCfg   *BinanceMarketCfg  `yaml:"binance.coin"`
}

type BinanceMode string

const (
	Spot   BinanceMode = "spot"
	Future BinanceMode = "future"
)

func NewAppConfigFromEnv() *AppConfig {
	reconnectPeriodM, err := strconv.Atoi(os.Getenv("binance.reconnect.period.m"))
	if err != nil {
		panic(err)
	}
	mode := BinanceMode(os.Getenv("binance.mode"))
	if mode != Spot && mode != Future {
		panic("unknown mode " + mode)
	}
	var spotCfg, usdCfg, coinCfg *BinanceMarketCfg
	if mode == Spot {
		spotCfg = NewBinanceMarketCfgFromEnv("binance.spot")
	} else {
		usdCfg = NewBinanceMarketCfgFromEnv("binance.usd")
		coinCfg = NewBinanceMarketCfgFromEnv("binance.coin")
	}
	return &AppConfig{
		Mode:             mode,
		ReconnectPeriodM: int16(reconnectPeriodM),
		CsCfg:            conf.NewCsRepoConfigFromEnv("socrates"),
		BinanceSpotCfg:   spotCfg,
		BinanceUSDCfg:    usdCfg,
		BinanceCoinCfg:   coinCfg,
	}
}

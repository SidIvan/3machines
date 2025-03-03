package conf

import (
	"DeltaReceiver/internal/common/conf"
	"os"
	"strconv"
)

type AppConfig struct {
	ReconnectPeriodM int16              `yaml:"binance.reconnect.period.m"`
	MongoRepoCfg     *MongoRepoConfig   `yaml:"mongo"`
	CsCfg            *conf.CsRepoConfig `yaml:"socrates"`
	BinanceSpotCfg   *BinanceMarketCfg  `yaml:"binance.spot"`
	BinanceUSDCfg    *BinanceMarketCfg  `yaml:"binance.usd"`
	BinanceCoinCfg   *BinanceMarketCfg  `yaml:"binance.coin"`
}

func NewAppConfigFromEnv() *AppConfig {
	reconnectPeriodM, err := strconv.Atoi(os.Getenv("binance.reconnect.period.m"))
	if err != nil {
		panic(err)
	}
	return &AppConfig{
		ReconnectPeriodM: int16(reconnectPeriodM),
		CsCfg:            conf.NewCsRepoConfigFromEnv("socrates"),
		MongoRepoCfg:     NewMongoRepoConfigFromEnv("mongo"),
		BinanceSpotCfg:   NewBinanceMarketCfgFromEnv("binance.spot"),
		BinanceUSDCfg:    NewBinanceMarketCfgFromEnv("binance.usd"),
		BinanceCoinCfg:   NewBinanceMarketCfgFromEnv("binance.coin"),
	}
}

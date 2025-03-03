package conf

import (
	mconf "DeltaReceiver/pkg/mongo/conf"
)

type MongoRepoConfig struct {
	MongoConfig    *mconf.MongoRepoConfig     `yaml:"config"`
	BinanceSpotCfg *BinanceMarketMongoRepoCfg `yaml:"binance.spot"`
	BinanceUSDCfg  *BinanceMarketMongoRepoCfg `yaml:"binance.usd"`
	BinanceCoinCfg *BinanceMarketMongoRepoCfg `yaml:"binance.coin"`
}

func NewMongoRepoConfigFromEnv(envPrefix string) *MongoRepoConfig {
	return &MongoRepoConfig{
		MongoConfig:    mconf.NewMongoRepoConfigFromEnv(envPrefix + ".config"),
		BinanceSpotCfg: NewBinanceMarketMongoRepoCfg(envPrefix + ".binance.spot"),
		BinanceUSDCfg:  NewBinanceMarketMongoRepoCfg(envPrefix + ".binance.usd"),
		BinanceCoinCfg: NewBinanceMarketMongoRepoCfg(envPrefix + ".binance.coin"),
	}
}

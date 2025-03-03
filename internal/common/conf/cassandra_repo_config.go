package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts          []string                `yaml:"hosts"`
	KeySpace       string                  `yaml:"binance.keyspace"`
	BinanceSpotCfg *BinanceMarketCsRepoCfg `yaml:"binance.spot"`
	BinanceUSDCfg  *BinanceMarketCsRepoCfg `yaml:"binance.usd"`
	BinanceCoinCfg *BinanceMarketCsRepoCfg `yaml:"binance.coin"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	return &CsRepoConfig{
		Hosts:          strings.Split(os.Getenv(envPrefix+".hosts"), ","),
		KeySpace:       os.Getenv(envPrefix + ".binance.keyspace"),
		BinanceSpotCfg: NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.spot"),
		BinanceUSDCfg:  NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.usd"),
		BinanceCoinCfg: NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.coin"),
	}
}

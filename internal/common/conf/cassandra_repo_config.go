package conf

import (
	"os"
	"strconv"
	"strings"
)

type CsRepoConfig struct {
	Hosts          []string                `yaml:"hosts"`
	Port           int                     `yaml:"port"`
	KeySpace       string                  `yaml:"binance.keyspace"`
	BinanceSpotCfg *BinanceMarketCsRepoCfg `yaml:"binance.spot"`
	BinanceUSDCfg  *BinanceMarketCsRepoCfg `yaml:"binance.usd"`
	BinanceCoinCfg *BinanceMarketCsRepoCfg `yaml:"binance.coin"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	port, err := strconv.Atoi(os.Getenv(envPrefix + ".port"))
	if err != nil {
		panic(err)
	}
	return &CsRepoConfig{
		Hosts:          strings.Split(os.Getenv(envPrefix+".hosts"), ","),
		Port:           port,
		KeySpace:       os.Getenv(envPrefix + ".binance.keyspace"),
		BinanceSpotCfg: NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.spot"),
		BinanceUSDCfg:  NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.usd"),
		BinanceCoinCfg: NewBinanceMarketCsRepoCfgFromEnv(envPrefix + ".binance.coin"),
	}
}

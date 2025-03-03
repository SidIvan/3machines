package conf

import (
	"DeltaReceiver/pkg/binance"
	"os"
	"strconv"
)

type BinanceMarketCfg struct {
	DataType             string                           `yaml:"data.type"`
	BinanceHttpCfg       *binance.BinanceHttpClientConfig `yaml:"client"`
	DeltasPipelineCfg    *WsPipelineCfg                   `yaml:"deltas"`
	BookTicksPipelineCfg *WsPipelineCfg                   `yaml:"book.ticks"`
	ExchangeInfoUpdPerM  int                              `yaml:"exchange.info.update.period.m"`
	SnapshotsDepth       int                              `yaml:"snapshots.depth"`
}

func NewBinanceMarketCfgFromEnv(envPrefix string) *BinanceMarketCfg {
	exchangeInfoUpdatePeriodM, err := strconv.Atoi(os.Getenv(envPrefix + ".exchange.info.update.period.m"))
	if err != nil {
		panic(err)
	}
	snapshotsDepth, err := strconv.Atoi(os.Getenv(envPrefix + ".snapshots.depth"))
	if err != nil {
		panic(err)
	}
	return &BinanceMarketCfg{
		BinanceHttpCfg:       binance.NewBinanceHttpClientConfigFromEnv(envPrefix + ".client"),
		DeltasPipelineCfg:    NewWsPipelineCfgFromEnv(envPrefix + ".deltas"),
		BookTicksPipelineCfg: NewWsPipelineCfgFromEnv(envPrefix + ".book.ticks"),
		ExchangeInfoUpdPerM:  exchangeInfoUpdatePeriodM,
		DataType:             os.Getenv(envPrefix + ".data.type"),
		SnapshotsDepth:       snapshotsDepth,
	}
}

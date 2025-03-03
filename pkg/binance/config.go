package binance

import (
	"DeltaReceiver/pkg/conf"
)

type BinanceHttpClientConfig struct {
	StreamBaseUriConfig *conf.BaseUriConfig `yaml:"stream.uri"`
	HttpBaseUriConfig   *conf.BaseUriConfig `yaml:"http.uri"`
}

func NewBinanceHttpClientConfigFromEnv(envPrefix string) *BinanceHttpClientConfig {
	return &BinanceHttpClientConfig{
		StreamBaseUriConfig: conf.NewBaseUriConfigFromEnv(envPrefix + ".stream.uri"),
		HttpBaseUriConfig:   conf.NewBaseUriConfigFromEnv(envPrefix + ".http.uri"),
	}
}

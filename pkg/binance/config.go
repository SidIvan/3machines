package binance

import "DeltaReceiver/pkg/conf"

type BinanceHttpClientConfig struct {
	StreamBaseUriConfig *conf.BaseUriConfig `yaml:"delta.stream.url"`
	HttpBaseUriConfig   *conf.BaseUriConfig `yaml:"http.base.uri.config"`
}

func NewBinanceHttpClientConfigFromEnv(envPrefix string) *BinanceHttpClientConfig {
	return &BinanceHttpClientConfig{
		StreamBaseUriConfig: conf.NewBaseUriConfigFromEnv(envPrefix + "_stream_uri"),
		HttpBaseUriConfig:   conf.NewBaseUriConfigFromEnv(envPrefix + "_http_uri"),
	}
}

package binance

import (
	"DeltaReceiver/pkg/conf"
	"os"
	"strconv"
)

type BinanceHttpClientConfig struct {
	StreamBaseUriConfig *conf.BaseUriConfig `yaml:"stream.uri"`
	HttpBaseUriConfig   *conf.BaseUriConfig `yaml:"http.uri"`
	UseAllTickersStream bool                `yaml:"use.all.tickers.stream"`
}

func NewBinanceHttpClientConfigFromEnv(envPrefix string) *BinanceHttpClientConfig {
	useAllTickersStream, err := strconv.ParseBool(os.Getenv(envPrefix + ".use.all.tickers.stream"))
	if err != nil {
		panic(err)
	}
	return &BinanceHttpClientConfig{
		StreamBaseUriConfig: conf.NewBaseUriConfigFromEnv(envPrefix + ".stream.uri"),
		HttpBaseUriConfig:   conf.NewBaseUriConfigFromEnv(envPrefix + ".http.uri"),
		UseAllTickersStream: useAllTickersStream,
	}
}

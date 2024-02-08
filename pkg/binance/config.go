package binance

import "DeltaReceiver/pkg/conf"

type BinanceHttpClientConfig struct {
	DeltaStreamBaseUriConfig *conf.BaseUriConfig `yaml:"delta.stream.url"`
	HttpBaseUriConfig        *conf.BaseUriConfig `yaml:"http.base.uri.config"`
	Pair2Period              map[string]int16    `yaml:"delta.pairs"`
	SnapshotPeriod           map[string]int16    `yaml:"snapshot.periods"`
}

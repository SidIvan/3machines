package web

import "DeltaReceiver/internal/conf"

type BinanceHttpClient struct {
	deltaURI string
}

func NewBinanceHttpClient(cfg *conf.BinanceHttpClientConfig) BinanceHttpClient {
	return BinanceHttpClient{
		deltaURI: cfg.DeltaStreamBaseUriConfig.GetBaseUri(),
	}
}

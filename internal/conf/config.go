package conf

import "DeltaReceiver/pkg/binance"

type AppConfig struct {
	BinanceHttpConfig *binance.BinanceHttpClientConfig `yaml:"binance.client"`
	GDBBatchSize      int                              `yaml:"global.db.batch.size"`
}

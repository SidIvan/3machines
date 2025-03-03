package conf

import "os"

type BinanceMarketMongoRepoCfg struct {
	DeltaColName      string `yaml:"delta.table"`
	SnapshotColName   string `yaml:"snapshot.table"`
	ExInfoColName     string `yaml:"exchange.info.table"`
	BookTickerColName string `yaml:"book.ticker.table"`
}

func NewBinanceMarketMongoRepoCfg(envPrefix string) *BinanceMarketMongoRepoCfg {
	return &BinanceMarketMongoRepoCfg{
		DeltaColName:      os.Getenv(envPrefix + ".delta.table"),
		SnapshotColName:   os.Getenv(envPrefix + ".snapshot.table"),
		ExInfoColName:     os.Getenv(envPrefix + ".exchange.info.table"),
		BookTickerColName: os.Getenv(envPrefix + ".book.ticker.table"),
	}
}

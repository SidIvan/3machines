package conf

import "os"

type BinanceMarketCsRepoCfg struct {
	DeltaTableName        string `yaml:"delta.table"`
	DeltaKeyTableName     string `yaml:"delta.key.table"`
	SnapshotTableName     string `yaml:"snapshot.table"`
	SnapshotKeyTableName  string `yaml:"snapshot.key.table"`
	BookTicksTableName    string `yaml:"book.ticks.table"`
	BookTicksKeyTableName string `yaml:"book.ticks.key.table"`
	ExchangeInfoTableName string `yaml:"exchange.info.table"`
}

func NewBinanceMarketCsRepoCfgFromEnv(envPrefix string) *BinanceMarketCsRepoCfg {
	return &BinanceMarketCsRepoCfg{
		DeltaTableName:        os.Getenv(envPrefix + ".delta.table"),
		DeltaKeyTableName:     os.Getenv(envPrefix + ".delta.key.table"),
		SnapshotTableName:     os.Getenv(envPrefix + ".snapshot.table"),
		SnapshotKeyTableName:  os.Getenv(envPrefix + ".snapshot.key.table"),
		BookTicksTableName:    os.Getenv(envPrefix + ".book.ticks.table"),
		BookTicksKeyTableName: os.Getenv(envPrefix + ".book.ticks.key.table"),
		ExchangeInfoTableName: os.Getenv(envPrefix + ".exchange.info.table"),
	}
}

package conf

import (
	"DeltaReceiver/internal/common/conf"
	"DeltaReceiver/pkg/binance"
	pconf "DeltaReceiver/pkg/conf"
	mconf "DeltaReceiver/pkg/mongo/conf"
)

type AppConfig struct {
	BinanceHttpConfig      *binance.BinanceHttpClientConfig `yaml:"binance.client"`
	GetFullSnapshotPeriodM int16                            `yaml:"get.full.snapshot.period.m"`
	FullSnapshotDepth      int                              `yaml:"full.snapshot.depth"`
	ReconnectPeriodM       int16                            `yaml:"reconnect.period.m"`
	LocalRepoCfg           *LocalRepoConfig                 `yaml:"local.repo.config"`
	GlobalRepoConfig       *conf.GlobalRepoConfig           `yaml:"global.repo.config"`
	ExchangeInfoUpdPerM    int                              `yaml:"ex.info.upd.per.m"`
	DwarfUrl               *pconf.BaseUriConfig             `yaml:"dwarf.url"`
}

type LocalRepoConfig struct {
	DeltaColName      string                 `yaml:"delta.collection.name"`
	SnapshotColName   string                 `yaml:"snapshot.collection.name"`
	ExInfoColName     string                 `yaml:"exchange.info.collection.name"`
	BookTickerColName string                 `yaml:"book.ticker.collection.name"`
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
}

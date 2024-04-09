package conf

import (
	"DeltaReceiver/pkg/binance"
	conf2 "DeltaReceiver/pkg/conf"
	"DeltaReceiver/pkg/mongo/conf"
)

type AppConfig struct {
	BinanceHttpConfig      *binance.BinanceHttpClientConfig `yaml:"binance.client"`
	GDBBatchSize           int                              `yaml:"global.db.batch.size"`
	GetFullSnapshotPeriodM int16                            `yaml:"get.full.snapshot.period.m"`
	FullSnapshotDepth      int                              `yaml:"full.snapshot.depth"`
	ReconnectPeriodM       int16                            `yaml:"reconnect.period.m"`
	LocalRepoCfg           *LocalRepoConfig                 `yaml:"local.repo.config"`
	GlobalRepoConfig       *GlobalRepoConfig                `yaml:"global.repo.config"`
	ExchangeInfoUpdPerM    int                              `yaml:"ex.info.upd.per.m"`
}

type LocalRepoConfig struct {
	DeltaColName      string                `yaml:"delta.collection.name"`
	SnapshotColName   string                `yaml:"snapshot.collection.name"`
	ExInfoColName     string                `yaml:"exchange.info.collection.name"`
	BookTickerColName string                `yaml:"book.ticker.collection.name"`
	MongoConfig       *conf.MongoRepoConfig `yaml:"mongo"`
}

type GlobalRepoConfig struct {
	URI               *conf2.BaseUriConfig `yaml:"base.uri"`
	TimeoutS          int                  `yaml:"timeoutS"`
	DatabaseName      string               `yaml:"database.name"`
	DeltaTable        string               `yaml:"delta.table.name"`
	SnapshotTable     string               `yaml:"snapshot.table.name"`
	ExchangeInfoTable string               `yaml:"ex.info.table"`
	BookTickerTable   string               `yaml:"binance.order.books.tops"`
}

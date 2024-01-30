package conf

import (
	"DeltaReceiver/pkg/binance"
	"DeltaReceiver/pkg/mongo/conf"
)

type AppConfig struct {
	BinanceHttpConfig      *binance.BinanceHttpClientConfig `yaml:"binance.client"`
	GDBBatchSize           int                              `yaml:"global.db.batch.size"`
	GetFullSnapshotPeriodM int16                            `yaml:"get.full.snapshot.period.m"`
	LocalRepoCfg           *LocalRepoConfig                 `yaml:"local.repo.config"`
	GlobalRepoConfig       *GlobalRepoConfig                `yaml:"global.repo.config"`
}

type LocalRepoConfig struct {
	DeltaColName    string                `yaml:"delta.collection.name"`
	SnapshotColName string                `yaml:"snapshot.collection.name"`
	MongoConfig     *conf.MongoRepoConfig `yaml:"mongo"`
}

type GlobalRepoConfig struct {
	TimeoutS int `yaml:"timeoutS"`
}

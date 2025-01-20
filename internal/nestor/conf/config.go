package conf

import (
	"DeltaReceiver/internal/common/conf"
	"DeltaReceiver/pkg/binance"
	pconf "DeltaReceiver/pkg/conf"
	mconf "DeltaReceiver/pkg/mongo/conf"
	"os"
	"strconv"
)

type AppConfig struct {
	BinanceHttpConfig   *binance.BinanceHttpClientConfig `yaml:"binance.client"`
	ReconnectPeriodM    int16                            `yaml:"reconnect.period.m"`
	LocalRepoCfg        *LocalRepoConfig                 `yaml:"local.repo.config"`
	ExchangeInfoUpdPerM int16                            `yaml:"ex.info.upd.per.m"`
	DwarfUrl            *pconf.BaseUriConfig             `yaml:"dwarf.url"`
	CsCfg               *conf.CsRepoConfig
}

func NewAppConfigFromEnv() *AppConfig {
	binanceClientConfig := binance.NewBinanceHttpClientConfigFromEnv("binance.client")
	localRepoConfig := NewLocalRepoConfigFromEnv("local.repo")
	reconnectPeriodM, err := strconv.Atoi(os.Getenv("binance.reconnect.period.m"))
	if err != nil {
		panic(err)
	}
	exchangeInfoUpdatePeriodM, err := strconv.Atoi(os.Getenv("binance.exchange.info.update.period.m"))
	if err != nil {
		panic(err)
	}
	csConfig := conf.NewCsRepoConfigFromEnv("global.repo")
	dwarfCfg := pconf.NewBaseUriConfigFromEnv("dwarf.uri")
	return &AppConfig{
		BinanceHttpConfig:   binanceClientConfig,
		LocalRepoCfg:        localRepoConfig,
		ReconnectPeriodM:    int16(reconnectPeriodM),
		ExchangeInfoUpdPerM: int16(exchangeInfoUpdatePeriodM),
		DwarfUrl:            dwarfCfg,
		CsCfg:               csConfig,
	}
}

type LocalRepoConfig struct {
	DeltaColName      string                 `yaml:"delta.collection.name"`
	SnapshotColName   string                 `yaml:"snapshot.collection.name"`
	ExInfoColName     string                 `yaml:"exchange.info.collection.name"`
	BookTickerColName string                 `yaml:"book.ticker.collection.name"`
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
}

func NewLocalRepoConfigFromEnv(envPrefix string) *LocalRepoConfig {
	deltaColName := os.Getenv(envPrefix + ".delta.table.name")
	snapshotColName := os.Getenv(envPrefix + ".snapshot.table.name")
	exInfoColName := os.Getenv(envPrefix + ".exchange.info.table.name")
	bookTickerColName := os.Getenv(envPrefix + ".book.ticks.table.name")
	mongoCfg := mconf.NewMongoRepoConfigFromEnv(envPrefix)
	return &LocalRepoConfig{
		DeltaColName:      deltaColName,
		SnapshotColName:   snapshotColName,
		ExInfoColName:     exInfoColName,
		BookTickerColName: bookTickerColName,
		MongoConfig:       mongoCfg,
	}
}

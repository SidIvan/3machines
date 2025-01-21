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
	ReconnectPeriodM    int16                            `yaml:"binance.reconnect.period.m"`
	ExchangeInfoUpdPerM int16                            `yaml:"binance.exchange.info.update.period.m"`
	LocalRepoCfg        *LocalRepoConfig                 `yaml:"local.repo"`
	DwarfUrl            *pconf.BaseUriConfig             `yaml:"dwarf.url"`
	CsCfg               *conf.CsRepoConfig               `yaml:"glogal.repo.binace"`
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
	csConfig := conf.NewCsRepoConfigFromEnv("socrates")
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
	DeltaColName      string                 `yaml:"delta.table"`
	SnapshotColName   string                 `yaml:"snapshot.table"`
	ExInfoColName     string                 `yaml:"exchange.info.table"`
	BookTickerColName string                 `yaml:"book.ticker.table"`
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
}

func NewLocalRepoConfigFromEnv(envPrefix string) *LocalRepoConfig {
	deltaColName := os.Getenv(envPrefix + ".delta.table")
	snapshotColName := os.Getenv(envPrefix + ".snapshot.table")
	exInfoColName := os.Getenv(envPrefix + ".exchange.info.table")
	bookTickerColName := os.Getenv(envPrefix + ".book.ticks.table")
	mongoCfg := mconf.NewMongoRepoConfigFromEnv(envPrefix)
	return &LocalRepoConfig{
		DeltaColName:      deltaColName,
		SnapshotColName:   snapshotColName,
		ExInfoColName:     exInfoColName,
		BookTickerColName: bookTickerColName,
		MongoConfig:       mongoCfg,
	}
}

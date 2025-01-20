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

func NewAppConfigFromEnv(envPrefix string) *AppConfig {
	binanceClientConfig := binance.NewBinanceHttpClientConfigFromEnv(envPrefix + "_binance")
	localRepoConfig := NewLocalRepoConfigFromEnv(envPrefix + "_local_repo")
	reconnectPeriodM, err := strconv.Atoi(os.Getenv(envPrefix + "_reconnect_period_m"))
	if err != nil {
		panic(err)
	}
	exchangeInfoUpdatePeriodM, err := strconv.Atoi(os.Getenv(envPrefix + "_exchange_info_update_period_m"))
	if err != nil {
		panic(err)
	}
	csConfig := conf.NewCsRepoConfigFromEnv(envPrefix + "_global_repo")
	dwarfCfg := pconf.NewBaseUriConfigFromEnv(envPrefix + "_dwarf")
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
	deltaColName := os.Getenv(envPrefix + "_delta_table_name")
	snapshotColName := os.Getenv(envPrefix + "_snapshot_table_name")
	exInfoColName := os.Getenv(envPrefix + "_exchange_info_table_name")
	bookTickerColName := os.Getenv(envPrefix + "_book_ticks_table_name")
	mongoCfg := mconf.NewMongoRepoConfigFromEnv(envPrefix)
	return &LocalRepoConfig{
		DeltaColName:      deltaColName,
		SnapshotColName:   snapshotColName,
		ExInfoColName:     exInfoColName,
		BookTickerColName: bookTickerColName,
		MongoConfig:       mongoCfg,
	}
}

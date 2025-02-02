package conf

import (
	"DeltaReceiver/internal/common/conf"
	"os"
	"strconv"
)

type AppConfig struct {
	ZkCfg           *ZkConfig          `yaml:"zk"`
	B2Cfg           *B2Config          `yaml:"b2"`
	SocratesCfg     *conf.CsRepoConfig `yaml:"socrates"`
	DeltaWorkers    int                `yaml:"workers.binance.deltas"`
	BookTicksWorker int                `yaml:"workers.binance.book.ticks"`
	SnapshotsWorker int                `yaml:"workers.binance.snapshots"`
}

func AppConfigFromEnv(prefix string) *AppConfig {
	deltaWorkers, err := strconv.Atoi(os.Getenv(prefix + ".workers.binance.deltas"))
	if err != nil {
		panic(err)
	}
	bookTicksWorkers, err := strconv.Atoi(os.Getenv(prefix + ".workers.binance.book.ticks"))
	if err != nil {
		panic(err)
	}
	snapshotsWorkers, err := strconv.Atoi(os.Getenv(prefix + ".workers.binance.snapshots"))
	if err != nil {
		panic(err)
	}
	return &AppConfig{
		ZkCfg:           ZkConfigFromEnv("zk"),
		B2Cfg:           B2ConfigFromEnv("b2"),
		SocratesCfg:     conf.NewCsRepoConfigFromEnv("socrates"),
		DeltaWorkers:    deltaWorkers,
		BookTicksWorker: bookTicksWorkers,
		SnapshotsWorker: snapshotsWorkers,
	}
}

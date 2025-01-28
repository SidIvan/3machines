package conf

import (
	"DeltaReceiver/internal/common/conf"
	"os"
	"strconv"
)

type AppConfig struct {
	ZkCfg        *ZkConfig          `yaml:"zk"`
	B2Cfg        *B2Config          `yaml:"b2"`
	SocratesCfg  *conf.CsRepoConfig `yaml:"socrates"`
	DeltaWorkers int                `yaml:"workers.binance.deltas"`
}

func AppConfigFromEnv(prefix string) *AppConfig {
	deltaWorkers, err := strconv.Atoi(os.Getenv(prefix + ".workers.binance.deltas"))
	if err != nil {
		panic(err)
	}
	return &AppConfig{
		ZkCfg:        ZkConfigFromEnv("zk"),
		B2Cfg:        B2ConfigFromEnv("b2"),
		SocratesCfg:  conf.NewCsRepoConfigFromEnv("socrates"),
		DeltaWorkers: deltaWorkers,
	}
}

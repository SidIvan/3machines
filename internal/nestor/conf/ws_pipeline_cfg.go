package conf

import (
	"os"
	"strconv"
)

type WsPipelineCfg struct {
	NumWorkers int `yaml:"num.workers"`
	BatchSize  int `yaml:"batch.size"`
}

func NewWsPipelineCfgFromEnv(envPrefix string) *WsPipelineCfg {
	numWorkers, err := strconv.Atoi(os.Getenv(envPrefix + ".num.workers"))
	if err != nil {
		panic(err)
	}
	batchSize, err := strconv.Atoi(os.Getenv(envPrefix + ".batch.size"))
	if err != nil {
		panic(err)
	}
	return &WsPipelineCfg{
		NumWorkers: numWorkers,
		BatchSize:  batchSize,
	}
}

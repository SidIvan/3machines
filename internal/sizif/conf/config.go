package conf

import (
	gconf "DeltaReceiver/internal/common/conf"
)

type AppConfig struct {
	ParquetStorageCfg       *ParquetConfig          `yaml:"parquet.storage"`
	GlobalRepoConfig        *gconf.GlobalRepoConfig `yaml:"global.repo.config"`
	NumWorkerThreads        int                     `yaml:"num.parallel.processes"`
	IsDeleteProcessedDeltas bool                    `yaml:"delete.processed.deltas"`
	ProcessDeltasFrom       string                  `yaml:"process.deltas.from"`
}

type ParquetConfig struct {
	BasePath string `yaml:"base.path"`
}

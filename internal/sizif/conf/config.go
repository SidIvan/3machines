package conf

import (
	gconf "DeltaReceiver/internal/common/conf"
)

type AppConfig struct {
	ParquetStorageCfg       *ParquetConfig          `yaml:"parquet.storage"`
	GlobalRepoConfig        *gconf.GlobalRepoConfig `yaml:"global.common.config"`
	NumWorkerThreads        int                     `yaml:"num.parallel.processes"`
	IsDeleteProcessedDeltas bool                    `yaml:"delete.processed.deltas"`
}

type ParquetConfig struct {
	BasePath string `yaml:"base.path"`
}

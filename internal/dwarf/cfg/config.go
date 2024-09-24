package cfg

import (
	mconf "DeltaReceiver/pkg/mongo/conf"
)

type HolesStorageConfig struct {
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
	DeltaHolesColName string                 `yaml:"delta.holes.col.name"`
}

type AppConfig struct {
	HolesStorageCfg  *HolesStorageConfig `yaml:"holes.storage.cfg"`
	NestorListenPort int                 `yaml:"nestor.listen.port"`
}

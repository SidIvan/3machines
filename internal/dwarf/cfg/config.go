package cfg

import (
	mconf "DeltaReceiver/pkg/mongo/conf"
	"os"
	"strconv"
)

type HolesStorageConfig struct {
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
	DeltaHolesColName string                 `yaml:"delta.holes.table"`
}

func NewHolesStorageConfigFromEnv(envPrefix string) *HolesStorageConfig {
	return &HolesStorageConfig{
		DeltaHolesColName: os.Getenv("delta.holes.table"),
		MongoConfig:       mconf.NewMongoRepoConfigFromEnv(envPrefix),
	}
}

type AppConfig struct {
	HolesStorageCfg *HolesStorageConfig `yaml:"holes.storage"`
	ListenPort      int                 `yaml:"in.port"`
}

func NewAppConfigFromEnv() *AppConfig {
	listenInPort, err := strconv.Atoi(os.Getenv("in.port"))
	if err != nil {
		panic(err)
	}
	return &AppConfig{
		HolesStorageCfg: NewHolesStorageConfigFromEnv("holes.storage"),
		ListenPort:      listenInPort,
	}
}

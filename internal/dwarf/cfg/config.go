package cfg

import (
	mconf "DeltaReceiver/pkg/mongo/conf"
	"os"
	"strconv"
)

type HolesStorageConfig struct {
	MongoConfig       *mconf.MongoRepoConfig `yaml:"mongo"`
	DeltaHolesColName string                 `yaml:"spot.delta.table"`
}

func NewHolesStorageConfigFromEnv(envPrefix string) *HolesStorageConfig {
	return &HolesStorageConfig{
		DeltaHolesColName: os.Getenv("spot.delta.table"),
		MongoConfig:       mconf.NewMongoRepoConfigFromEnv(envPrefix + ".mongo"),
	}
}

type AppConfig struct {
	HolesStorageCfg *HolesStorageConfig `yaml:"holes.storage"`
	ListenPort      int                 `yaml:"http.api.port"`
}

func NewAppConfigFromEnv() *AppConfig {
	listenInPort, err := strconv.Atoi(os.Getenv("http.api.port"))
	if err != nil {
		panic(err)
	}
	return &AppConfig{
		HolesStorageCfg: NewHolesStorageConfigFromEnv("holes.storage"),
		ListenPort:      listenInPort,
	}
}

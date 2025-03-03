package conf

import (
	"DeltaReceiver/pkg/conf"
	"os"
	"strconv"
)

type MongoRepoConfig struct {
	TimeoutS       int64               `yaml:"timeout.s"`
	URI            *conf.BaseUriConfig `yaml:"uri"`
	DatabaseName   string              `yaml:"database.name"`
	NumConnRetries int8                `yaml:"num.connection.retries"`
}

func NewMongoRepoConfigFromEnv(envPrefix string) *MongoRepoConfig {
	uriConfig := conf.NewBaseUriConfigFromEnv(envPrefix + ".uri")
	timeoutS, err := strconv.Atoi(os.Getenv(envPrefix + ".timeout.s"))
	if err != nil {
		panic(err)
	}
	dbName := os.Getenv(envPrefix + ".database")
	numConnRetries, err := strconv.Atoi(os.Getenv(envPrefix + ".num.connection.retries"))
	if err != nil {
		panic(err)
	}
	return &MongoRepoConfig{
		TimeoutS:       int64(timeoutS),
		URI:            uriConfig,
		DatabaseName:   dbName,
		NumConnRetries: int8(numConnRetries),
	}
}
package conf

import (
	"DeltaReceiver/pkg/conf"
	"os"
	"strconv"
)

type MongoRepoConfig struct {
	TimeoutS       int64               `yaml:"timeout.sec"`
	URI            *conf.BaseUriConfig `yaml:"uri"`
	DatabaseName   string              `yaml:"database.name"`
	NumConnRetries int8                `yaml:"num.conn.retries"`
}

func NewMongoRepoConfigFromEnv(envPrefix string) *MongoRepoConfig {
	uriConfig := conf.NewBaseUriConfigFromEnv(envPrefix + "_uri")
	timeoutS, err := strconv.Atoi(os.Getenv(envPrefix + "_timeout_s"))
	if err != nil {
		panic(err)
	}
	dbName := os.Getenv(envPrefix + "_database")
	numConnRetries, err := strconv.Atoi(os.Getenv(envPrefix + "_num_connection_retries"))
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
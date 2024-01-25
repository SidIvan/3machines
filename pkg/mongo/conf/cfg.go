package conf

import (
	"DeltaReceiver/pkg/conf"
)

type MongoRepoConfig struct {
	TimeoutS       int64              `yaml:"timeout.sec"`
	URI            conf.BaseUriConfig `yaml:"uri"`
	DatabaseName   string             `yaml:"database.name"`
	NumConnRetries int8               `yaml:"num.conn.retries"`
}

func (s *MongoRepoConfig) getURI() string {
	return s.URI.GetBaseUri()
}

package conf

import (
	"os"
	"strconv"
	"strings"
)

type ZkConfig struct {
	Servers         []string `yaml:"servers"`
	SessionTimeoutS int64    `yaml:"session.timeout.s"`
}

func ZkConfigFromEnv(prefix string) *ZkConfig {
	sessionTimeout, err := strconv.ParseInt(os.Getenv(prefix+".session.timeout.s"), 10, 64)
	if err != nil {
		panic(err)
	}
	return &ZkConfig{
		Servers:         strings.Split(os.Getenv(prefix+".servers"), ","),
		SessionTimeoutS: sessionTimeout,
	}
}

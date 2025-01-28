package conf

import "os"

type B2Config struct {
	Account string
	Key     string
	Bucket  string `yaml:"bucket"`
}

func B2ConfigFromEnv(prefix string) *B2Config {
	return &B2Config{
		Account: os.Getenv(prefix + ".authorization.account"),
		Key:     os.Getenv(prefix + ".authorization.key"),
		Bucket:  os.Getenv(prefix + ".bucket"),
	}
}

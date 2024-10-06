package conf

import "fmt"

type BaseUriConfig struct {
	Schema string `yaml:"schema"`
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
}

func (cfg *BaseUriConfig) GetEndpoint() string {
	return fmt.Sprintf("%s%s", cfg.Schema, cfg.Host)
}

func (cfg *BaseUriConfig) GetBaseUri() string {
	return fmt.Sprintf("%s%s:%d/", cfg.Schema, cfg.Host, cfg.Port)
}

func (cfg *BaseUriConfig) GetAddress() string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}

package conf

import (
	"DeltaReceiver/pkg/conf"
)

type S3Cfg struct {
	URICfg *conf.BaseUriConfig `yaml:"uri"`
	Region string              `yaml:"region"`
	Id     string              `yaml:"id"`
	Secret string              `yaml:"secret"`
}

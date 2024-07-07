package clickhouse

import "DeltaReceiver/pkg/conf"

type ChPoolCfg struct {
	UriConf      *conf.BaseUriConfig `yaml:"base.uri"`
	DialTimeoutS int64               `yaml:"dial.timeout.s"`
	ReadTimeoutS int64               `yaml:"read.timeout.s"`
	ChMaxConns   int32               `yaml:"ch.max.conns"`
	ChMinConns   int32               `yaml:"ch.min.conns"`
	User         string              `yaml:"user"`
	Password     string              `yaml:"password"`
}

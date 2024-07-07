package conf

import (
	"DeltaReceiver/pkg/clickhouse"
)

type GlobalRepoConfig struct {
	ChPoolCfg         *clickhouse.ChPoolCfg `yaml:"ch.pool"`
	TimeoutS          int                   `yaml:"timeoutS"`
	DatabaseName      string                `yaml:"database.name"`
	DeltaTable        string                `yaml:"delta.table.name"`
	SnapshotTable     string                `yaml:"snapshot.table.name"`
	ExchangeInfoTable string                `yaml:"ex.info.table"`
	BookTickerTable   string                `yaml:"book.ticker.table"`
}

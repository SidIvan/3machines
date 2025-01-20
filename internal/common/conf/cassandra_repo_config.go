package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts                 []string `yaml:"hosts"`
	KeySpace              string   `yaml:"keyspace"`
	DeltaTableName        string   `yaml:"delta.table.name"`
	SnapshotTableName     string   `yaml:"snapshot.table.name"`
	BookTicksTableName    string   `yaml:"book.ticks.table.name"`
	ExchangeInfoTableName string   `yaml:"exchange.info.table.name"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	hosts := strings.Split(os.Getenv(envPrefix+".hosts"), ",")
	keySpace := os.Getenv(envPrefix + ".keyspace")
	deltaTableName := os.Getenv(envPrefix + ".delta.table.name")
	snapshotTableName := os.Getenv(envPrefix + ".snapshot.table.name")
	bookTicksTableName := os.Getenv(envPrefix + ".book.ticks.table.name")
	exchangeInfoTableName := os.Getenv(envPrefix + ".exchange.info.table.name")
	return &CsRepoConfig{
		Hosts:                 hosts,
		KeySpace:              keySpace,
		DeltaTableName:        deltaTableName,
		SnapshotTableName:     snapshotTableName,
		BookTicksTableName:    bookTicksTableName,
		ExchangeInfoTableName: exchangeInfoTableName,
	}
}

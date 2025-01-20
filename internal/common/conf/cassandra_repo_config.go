package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts                 []string `yaml:"hosts"`
	KeySpace              string   `yaml:"binace.keyspace"`
	DeltaTableName        string   `yaml:"binace.delta.table.name"`
	SnapshotTableName     string   `yaml:"binace.snapshot.table.name"`
	BookTicksTableName    string   `yaml:"binace.book.ticks.table.name"`
	ExchangeInfoTableName string   `yaml:"binace.exchange.info.table.name"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	hosts := strings.Split(os.Getenv(envPrefix+".hosts"), ",")
	keySpace := os.Getenv(envPrefix + ".binace.keyspace")
	deltaTableName := os.Getenv(envPrefix + ".binace.delta.table.name")
	snapshotTableName := os.Getenv(envPrefix + ".binace.snapshot.table.name")
	bookTicksTableName := os.Getenv(envPrefix + ".binace.book.ticks.table.name")
	exchangeInfoTableName := os.Getenv(envPrefix + ".binace.exchange.info.table.name")
	return &CsRepoConfig{
		Hosts:                 hosts,
		KeySpace:              keySpace,
		DeltaTableName:        deltaTableName,
		SnapshotTableName:     snapshotTableName,
		BookTicksTableName:    bookTicksTableName,
		ExchangeInfoTableName: exchangeInfoTableName,
	}
}

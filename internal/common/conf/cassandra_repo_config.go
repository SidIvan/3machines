package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts                 []string `yaml:"hosts"`
	KeySpace              string   `yaml:"binace.keyspace"`
	DeltaTableName        string   `yaml:"binace.delta.table"`
	SnapshotTableName     string   `yaml:"binace.snapshot.table"`
	BookTicksTableName    string   `yaml:"binace.book.ticks.table"`
	ExchangeInfoTableName string   `yaml:"binace.exchange.info.table"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	hosts := strings.Split(os.Getenv(envPrefix+".hosts"), ",")
	keySpace := os.Getenv(envPrefix + ".binace.keyspace")
	deltaTableName := os.Getenv(envPrefix + ".binace.delta.table")
	snapshotTableName := os.Getenv(envPrefix + ".binace.snapshot.table")
	bookTicksTableName := os.Getenv(envPrefix + ".binace.book.ticks.table")
	exchangeInfoTableName := os.Getenv(envPrefix + ".binace.exchange.info.table")
	return &CsRepoConfig{
		Hosts:                 hosts,
		KeySpace:              keySpace,
		DeltaTableName:        deltaTableName,
		SnapshotTableName:     snapshotTableName,
		BookTicksTableName:    bookTicksTableName,
		ExchangeInfoTableName: exchangeInfoTableName,
	}
}

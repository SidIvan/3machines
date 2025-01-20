package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts                 []string
	KeySpace              string
	DeltaTableName        string
	SnapshotTableName     string
	BookTicksTableName    string
	ExchangeInfoTableName string
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

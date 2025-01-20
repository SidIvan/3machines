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
	hosts := strings.Split(os.Getenv(envPrefix+"_hosts"), ",")
	keySpace := os.Getenv(envPrefix + "_keyspace")
	deltaTableName := os.Getenv(envPrefix + "_delta_table_name")
	snapshotTableName := os.Getenv(envPrefix + "_snapshot_table_name")
	bookTicksTableName := os.Getenv(envPrefix + "_book_ticks_table_name")
	exchangeInfoTableName := os.Getenv(envPrefix + "_exchange_info_table_name")
	return &CsRepoConfig{
		Hosts:                 hosts,
		KeySpace:              keySpace,
		DeltaTableName:        deltaTableName,
		SnapshotTableName:     snapshotTableName,
		BookTicksTableName:    bookTicksTableName,
		ExchangeInfoTableName: exchangeInfoTableName,
	}
}

package conf

import (
	"os"
	"strings"
)

type CsRepoConfig struct {
	Hosts                 []string `yaml:"hosts"`
	KeySpace              string   `yaml:"binace.keyspace"`
	DeltaTableName        string   `yaml:"binace.delta.table"`
	DeltaKeyTableName     string   `yaml:"binace.delta.key.table"`
	SnapshotTableName     string   `yaml:"binace.snapshot.table"`
	SnapshotKeyTableName  string   `yaml:"binace.snapshot.key.table"`
	BookTicksTableName    string   `yaml:"binace.book.ticks.table"`
	BookTicksKeyTableName string   `yaml:"binace.book.ticks.key.table"`
	ExchangeInfoTableName string   `yaml:"binace.exchange.info.table"`
}

func NewCsRepoConfigFromEnv(envPrefix string) *CsRepoConfig {
	hosts := strings.Split(os.Getenv(envPrefix+".hosts"), ",")
	keySpace := os.Getenv(envPrefix + ".binace.keyspace")
	deltaTableName := os.Getenv(envPrefix + ".binace.delta.table")
	deltaKeyTableName := os.Getenv(envPrefix + ".binace.delta.key.table")
	snapshotTableName := os.Getenv(envPrefix + ".binace.snapshot.table")
	snapshotKeyTableName := os.Getenv(envPrefix + ".binace.snapshot.key.table")
	bookTicksTableName := os.Getenv(envPrefix + ".binace.book.ticks.table")
	bookTicksKeyTableName := os.Getenv(envPrefix + ".binace.book.ticks.key.table")
	exchangeInfoTableName := os.Getenv(envPrefix + ".binace.exchange.info.table")
	return &CsRepoConfig{
		Hosts:                 hosts,
		KeySpace:              keySpace,
		DeltaTableName:        deltaTableName,
		DeltaKeyTableName:     deltaKeyTableName,
		SnapshotTableName:     snapshotTableName,
		SnapshotKeyTableName:  snapshotKeyTableName,
		BookTicksTableName:    bookTicksTableName,
		ExchangeInfoTableName: exchangeInfoTableName,
		BookTicksKeyTableName: bookTicksKeyTableName,
	}
}

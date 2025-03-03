package conf

import (
	"os"
	"strconv"
)

type BinanceMarketCfg struct {
	DeltaWorkers    int `yaml:"workers.binance.deltas"`
	BookTicksWorker int `yaml:"workers.binance.book.ticks"`
	SnapshotsWorker int `yaml:"workers.binance.snapshots"`
}

func NewBinanceMarketCfg(envPrefix string) *BinanceMarketCfg {
	deltaWorkers, err := strconv.Atoi(os.Getenv(envPrefix + ".workers.binance.deltas"))
	if err != nil {
		panic(err)
	}
	bookTicksWorkers, err := strconv.Atoi(os.Getenv(envPrefix + ".workers.binance.book.ticks"))
	if err != nil {
		panic(err)
	}
	snapshotsWorkers, err := strconv.Atoi(os.Getenv(envPrefix + ".workers.binance.snapshots"))
	if err != nil {
		panic(err)
	}
	return &BinanceMarketCfg{
		DeltaWorkers:    deltaWorkers,
		BookTicksWorker: bookTicksWorkers,
		SnapshotsWorker: snapshotsWorkers,
	}
}

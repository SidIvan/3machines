package metrics

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type Metrics struct {
	logger                     *zap.Logger
	NumReceivedDeltas          map[model.Symbol]prometheus.Counter
	NumDeltasSuccessReconnects map[model.Symbol]prometheus.Counter
	NumDeltasFailedReconnects  map[model.Symbol]prometheus.Counter
	NumReceivedSnapshotParts   map[model.Symbol]prometheus.Counter
}

func NewMetrics(cfg *conf.AppConfig) *Metrics {
	metrics := Metrics{
		logger:                     log.GetLogger("PrometheusMetricsHandler"),
		NumReceivedDeltas:          make(map[model.Symbol]prometheus.Counter),
		NumDeltasSuccessReconnects: make(map[model.Symbol]prometheus.Counter),
		NumDeltasFailedReconnects:  make(map[model.Symbol]prometheus.Counter),
		NumReceivedSnapshotParts:   make(map[model.Symbol]prometheus.Counter),
	}
	for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
		metrics.NumReceivedDeltas[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "binance_deltas",
			Name:      fmt.Sprintf("received_deltas_counter_%s", symbol),
		})
	}
	for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
		metrics.NumReceivedDeltas[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "binance_deltas",
			Name:      fmt.Sprintf("success_deltas_reconnects_%s", symbol),
		})
	}
	for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
		metrics.NumReceivedDeltas[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "binance_deltas",
			Name:      fmt.Sprintf("failed_deltas_reconnects_%s", symbol),
		})
	}
	for symbol, _ := range cfg.BinanceHttpConfig.SnapshotPeriod {
		metrics.NumReceivedDeltas[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "binance_snapshots",
			Name:      fmt.Sprintf("received_snapshot_part_ctr_%s", symbol),
		})
	}
	return &metrics
}

func (s *Metrics) IncreaseDeltaCtr(symbol model.Symbol, delta int) {
	if delta < 0 {
		s.logger.Warn("attempt to decrease ctr")
		return
	}
	s.NumReceivedDeltas[symbol].Add(float64(delta))
}

func (s *Metrics) IncrementSuccessDeltaReconnectCtr(symbol model.Symbol) {
	s.NumDeltasSuccessReconnects[symbol].Inc()
}

func (s *Metrics) IncrementFailedDeltaReconnectCtr(symbol model.Symbol) {
	s.NumDeltasFailedReconnects[symbol].Inc()
}

func (s *Metrics) IncreaseSnapshotPairCtr(symbol model.Symbol, delta int) {
	if delta < 0 {
		s.logger.Warn("attempt to decrease ctr")
		return
	}
	s.NumReceivedSnapshotParts[symbol].Add(float64(delta))
}

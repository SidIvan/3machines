package metrics

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/internal/svc"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"go.uber.org/zap"
	"strings"
)

type Metrics struct {
	logger    *zap.Logger
	deltasM   *deltaMetrics
	ticksM    *ticksMetrics
	snapshotM *snapshotMetrics
}

func NewMetrics(cfg *conf.AppConfig) *Metrics {
	metrics := Metrics{
		logger:    log.GetLogger("PrometheusMetricsHandler"),
		deltasM:   newDeltaMetrics(),
		ticksM:    newTicksMetrics(),
		snapshotM: newSnapshotMetrics(),
	}
	//for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
	//	metrics.NumReceivedDeltas[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
	//		Namespace: "binance_deltas",
	//		Name:      fmt.Sprintf("received_deltas_counter_%s", symbol),
	//	})
	//}
	//for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
	//	metrics.NumDeltasSuccessReconnects[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
	//		Namespace: "binance_deltas",
	//		Name:      fmt.Sprintf("success_deltas_reconnects_%s", symbol),
	//	})
	//}
	//for symbol, _ := range cfg.BinanceHttpConfig.Pair2Period {
	//	metrics.NumDeltasFailedReconnects[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
	//		Namespace: "binance_deltas",
	//		Name:      fmt.Sprintf("failed_deltas_reconnects_%s", symbol),
	//	})
	//}
	//for symbol, _ := range cfg.BinanceHttpConfig.SnapshotPeriod {
	//	metrics.NumReceivedSnapshotParts[model.SymbolFromString(symbol)] = promauto.NewCounter(prometheus.CounterOpts{
	//		Namespace: "binance_snapshots",
	//		Name:      fmt.Sprintf("received_snapshot_part_ctr_%s", symbol),
	//	})
	//}
	return &metrics
}

func (s *Metrics) ProcessDeltaMetrics(deltas []model.Delta, event svc.TypeOfEvent) {
	s.deltasM.ProcessMetrics(deltas, event)
}

func (s *Metrics) ProcessTickMetrics(ticks []bmodel.SymbolTick, event svc.TypeOfEvent) {
	s.ticksM.ProcessMetrics(ticks, event)
}

func (s *Metrics) ProcessSnapshotMetrics(snapshot []model.DepthSnapshotPart, event svc.TypeOfEvent) {
	s.snapshotM.ProcessMetrics(snapshot, event)
}

func (s *Metrics) UpdateMetrics(symbolInfos []bmodel.SymbolInfo) {
	var symbols []string
	for _, symbol := range symbolInfos {
		symbols = append(symbols, symbol.Symbol)
	}
	s.deltasM.updateActiveMetrics(symbols)
	s.ticksM.updateActiveMetrics(symbols)
	s.snapshotM.updateActiveMetrics(symbols)
}

func getMetricKey(symbol string) string {
	return strings.ToUpper(symbol)
}

package metrics

import (
	model2 "DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/svc"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"strings"

	"go.uber.org/zap"
)

type Metrics struct {
	logger    *zap.Logger
	snapshotM *snapshotMetrics
	exInfoM   *exInfoMetrics
}

func NewMetrics() *Metrics {
	metrics := Metrics{
		logger:    log.GetLogger("PrometheusMetricsHandler"),
		snapshotM: newSnapshotMetrics(),
		exInfoM:   newExInfoMetrics(),
	}
	return &metrics
}

func (s *Metrics) ProcessSnapshotMetrics(snapshot []model2.DepthSnapshotPart, event svc.TypeOfEvent) {
	s.snapshotM.ProcessMetrics(snapshot, event)
}

func (s *Metrics) ProcessExInfoMetrics(event svc.TypeOfEvent) {
	s.exInfoM.ProcessMetrics(event)
}

func (s *Metrics) UpdateMetrics(symbolInfos []bmodel.SymbolInfo) {
	var symbols []string
	for _, symbol := range symbolInfos {
		symbols = append(symbols, symbol.Symbol)
	}
	s.snapshotM.updateActiveMetrics(symbols)
	s.exInfoM.updateActiveMetrics()
}

func getMetricKey(symbol string) string {
	return strings.ToUpper(symbol)
}

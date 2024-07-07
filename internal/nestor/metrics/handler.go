package metrics

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/conf"
	model2 "DeltaReceiver/internal/nestor/model"
	"DeltaReceiver/internal/nestor/svc"
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
	exInfoM   *exInfoMetrics
	systemM   *systemMetrics
}

func NewMetrics(cfg *conf.AppConfig) *Metrics {
	metrics := Metrics{
		logger:    log.GetLogger("PrometheusMetricsHandler"),
		deltasM:   newDeltaMetrics(),
		ticksM:    newTicksMetrics(),
		snapshotM: newSnapshotMetrics(),
		exInfoM:   newExInfoMetrics(),
		systemM:   newSystemMetrics(),
	}
	return &metrics
}

func (s *Metrics) ProcessDeltaMetrics(deltas []model.Delta, event svc.TypeOfEvent) {
	s.deltasM.ProcessMetrics(deltas, event)
}

func (s *Metrics) ProcessTickMetrics(ticks []bmodel.SymbolTick, event svc.TypeOfEvent) {
	s.ticksM.ProcessMetrics(ticks, event)
}

func (s *Metrics) ProcessSnapshotMetrics(snapshot []model2.DepthSnapshotPart, event svc.TypeOfEvent) {
	s.snapshotM.ProcessMetrics(snapshot, event)
}

func (s *Metrics) ProcessExInfoMetrics(event svc.TypeOfEvent) {
	s.exInfoM.ProcessMetrics(event)
}

func (s *Metrics) ProcessSystemMetrics() {
	s.systemM.ProcessMetrics()
}

func (s *Metrics) UpdateMetrics(symbolInfos []bmodel.SymbolInfo) {
	var symbols []string
	for _, symbol := range symbolInfos {
		symbols = append(symbols, symbol.Symbol)
	}
	s.systemM.updateMetrics()
	s.deltasM.updateActiveMetrics(symbols)
	s.ticksM.updateActiveMetrics(symbols)
	s.snapshotM.updateActiveMetrics(symbols)
	s.exInfoM.updateActiveMetrics()
}

func getMetricKey(symbol string) string {
	return strings.ToUpper(symbol)
}

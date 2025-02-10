package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const SocratesMetricsNamespace string = "socrates"

type CsStorageMetrics struct {
	metricsLabels                 map[string]string
	errorCounter                  prometheus.Counter
	dataBatchInsertLatencySummary prometheus.Summary
	insertQueryLatencySummary     prometheus.Summary
}

func NewCsStorageMetrics(storageName string) *CsStorageMetrics {
	return &CsStorageMetrics{
		metricsLabels: map[string]string{"table_name": storageName},
	}
}

func (s CsStorageMetrics) IncErrCount() {
	if s.errorCounter == nil {
		s.errorCounter = promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   SocratesMetricsNamespace,
			Name:        "insert_errors",
			ConstLabels: s.metricsLabels,
		})
	}
	s.errorCounter.Inc()
}

func (s CsStorageMetrics) UpdInsertDataBatchLatency(latencyMs int64) {
	if s.dataBatchInsertLatencySummary == nil {
		s.dataBatchInsertLatencySummary = promauto.NewSummary(prometheus.SummaryOpts{
			Namespace:   SocratesMetricsNamespace,
			Name:        "insert_latency",
			ConstLabels: s.metricsLabels,
			Objectives:  map[float64]float64{0.5: 0.01, 0.75: 0.01, 0.90: 0.01, 0.95: 0.01, 0.99: 0.01},
		})
	}
	s.dataBatchInsertLatencySummary.Observe(float64(latencyMs))
}

func (s CsStorageMetrics) UpdInsertQueryLatency(latencyMs int64) {
	if s.insertQueryLatencySummary == nil {
		s.insertQueryLatencySummary = promauto.NewSummary(prometheus.SummaryOpts{
			Namespace:   SocratesMetricsNamespace,
			Name:        "insert_latency",
			ConstLabels: s.metricsLabels,
			Objectives:  map[float64]float64{0.5: 0.01, 0.75: 0.01, 0.90: 0.01, 0.95: 0.01, 0.99: 0.01},
		})
	}
	s.insertQueryLatencySummary.Observe(float64(latencyMs))
}

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const NestorMetricsNamespace string = "nestor"

type CsStorageMetrics struct {
	errorCounter                  prometheus.Counter
	dataBatchInsertLatencySummary prometheus.Summary
	insertQueryLatencySummary     prometheus.Summary
}

func NewCsStorageMetrics(storageName string) *CsStorageMetrics {
	metricsLabels := map[string]string{"table_name": storageName}
	return &CsStorageMetrics{
		errorCounter: promauto.NewCounter(prometheus.CounterOpts{
			Namespace:   NestorMetricsNamespace,
			Name:        "insert_errors",
			ConstLabels: metricsLabels,
		}),
		dataBatchInsertLatencySummary: promauto.NewSummary(prometheus.SummaryOpts{
			Namespace:   NestorMetricsNamespace,
			Name:        "data_batch_insert_latency",
			ConstLabels: metricsLabels,
			Objectives:  map[float64]float64{0.5: 0.01, 0.75: 0.01, 0.90: 0.01, 0.95: 0.01, 0.99: 0.01},
		}),
		insertQueryLatencySummary: promauto.NewSummary(prometheus.SummaryOpts{
			Namespace:   NestorMetricsNamespace,
			Name:        "data_insert_latency",
			ConstLabels: metricsLabels,
			Objectives:  map[float64]float64{0.5: 0.01, 0.75: 0.01, 0.90: 0.01, 0.95: 0.01, 0.99: 0.01},
		}),
	}
}

func (s CsStorageMetrics) IncErrCount() {
	s.errorCounter.Inc()
}

func (s CsStorageMetrics) UpdInsertDataBatchLatency(latencyMs int64) {
	s.dataBatchInsertLatencySummary.Observe(float64(latencyMs))
}

func (s CsStorageMetrics) UpdInsertQueryLatency(latencyMs int64) {
	s.insertQueryLatencySummary.Observe(float64(latencyMs))
}

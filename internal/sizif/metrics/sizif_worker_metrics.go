package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const SizifMetricsNamespace = "sizif"

type SizifWorkerMetrics struct {
	invalidDataCounter prometheus.Counter
}

func NewSizifWorkerMetrics(dataType string) *SizifWorkerMetrics {
	return &SizifWorkerMetrics{
		invalidDataCounter: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: SizifMetricsNamespace,
			Name:      fmt.Sprintf("%s_invalid_data_files", dataType),
		}),
	}
}

func (s SizifWorkerMetrics) IncInvalidDataCounter() {
	s.invalidDataCounter.Inc()
}

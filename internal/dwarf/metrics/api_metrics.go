package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ApiMetrics struct {
	createDeltaHoleCounter prometheus.Counter
}

func (s *ApiMetrics) IncNumCallsCreateDeltaHole() {
	s.createDeltaHoleCounter.Inc()
}

const apiMetricsNamespace = "api"

func NewApiMetrics() *ApiMetrics {
	return &ApiMetrics{
		createDeltaHoleCounter: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: apiMetricsNamespace,
			Name:      "received_create_hole",
		}),
	}
}

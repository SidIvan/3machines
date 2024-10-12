package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ApiMetrics struct {
	createDeltaHoleCounters map[string]prometheus.Counter
}

func (s *ApiMetrics) IncNumCallsCreateDeltaHole(serviceName string) {
	if counter, ok := s.createDeltaHoleCounters[serviceName]; !ok {
		s.createDeltaHoleCounters[serviceName] = createCreateDeltaHoleCounter(serviceName)
	} else {
		counter.Inc()
	}
}

func createCreateDeltaHoleCounter(serviceName string) prometheus.Counter {
	return promauto.NewCounter(prometheus.CounterOpts{
		Namespace: apiMetricsNamespace,
		Subsystem: serviceName,
		Name:      "received_create_hole",
	})
}

const apiMetricsNamespace = "api"

func NewApiMetrics() *ApiMetrics {
	createDeltaHolesCounters := make(map[string]prometheus.Counter)
	createDeltaHolesCounters["nestor1"] = createCreateDeltaHoleCounter("nestor1")
	createDeltaHolesCounters["nestor2"] = createCreateDeltaHoleCounter("nestor2")
	createDeltaHolesCounters["nestor3"] = createCreateDeltaHoleCounter("nestor3")
	createDeltaHolesCounters["nestor4"] = createCreateDeltaHoleCounter("nestor4")
	return &ApiMetrics{
		createDeltaHoleCounters: createDeltaHolesCounters,
	}
}

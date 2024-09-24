package metrics

import "github.com/prometheus/client_golang/prometheus"

type ApiMetrics struct {
	createDeltaHoleCounter prometheus.Counter
}

func (s *ApiMetrics) IncNumCallsCreateDeltaHole() {
	s.createDeltaHoleCounter.Inc()
}

func NewApiMetrics() *ApiMetrics {
	return &ApiMetrics{}
}

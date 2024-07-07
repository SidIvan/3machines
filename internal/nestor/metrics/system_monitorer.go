package metrics

import (
	"DeltaReceiver/internal/nestor/svc"
	"github.com/pbnjay/memory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

type SystemMonitorer struct {
	metrics svc.MetricsHolder
}

func NewSystemMonitorer(metrics svc.MetricsHolder) *SystemMonitorer {
	return &SystemMonitorer{
		metrics: metrics,
	}
}

func (s *SystemMonitorer) CronUpdateMetrics() {
	for {
		s.metrics.ProcessSystemMetrics()
		time.Sleep(5 * time.Second)
	}
}

type systemMetrics struct {
	freeRamGauge prometheus.Gauge
}

func newSystemMetrics() *systemMetrics {
	return &systemMetrics{}
}

const SystemNamespace = "system"

func (s *systemMetrics) updateMetrics() {
	if s.freeRamGauge == nil {
		s.freeRamGauge = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: SystemNamespace,
			Name:      "free_ram_bytes",
		})
	}
}

func (s *systemMetrics) ProcessMetrics() {
	s.freeRamGauge.Set(float64(memory.FreeMemory()))
}

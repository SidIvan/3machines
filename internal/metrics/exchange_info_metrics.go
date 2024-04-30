package metrics

import (
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type exInfoMetrics struct {
	logger         *zap.Logger
	ReceivedExInfo prometheus.Counter
	SentExInfo     prometheus.Counter
	SavedExInfo    prometheus.Counter
}

func newExInfoMetrics() *exInfoMetrics {
	return &exInfoMetrics{
		logger: log.GetLogger("TicksMetricsImpl"),
	}
}

const ExInfoNamespace = "binance_exchange_info"

func (s *exInfoMetrics) updateActiveMetrics() {
	if s.ReceivedExInfo == nil {
		s.ReceivedExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "received_total",
		})
		s.SentExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "sent_total",
		})
		s.SavedExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "saved_total",
		})
	}
}

func (s *exInfoMetrics) ProcessMetrics(event svc.TypeOfEvent) {
	if event == svc.Receive {
		s.ReceivedExInfo.Inc()
	} else if event == svc.Send {
		s.SentExInfo.Inc()
	} else if event == svc.Save {
		s.SavedExInfo.Inc()
	}
}

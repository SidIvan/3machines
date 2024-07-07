package metrics

import (
	"DeltaReceiver/internal/nestor/svc"
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
		logger: log.GetLogger("ExchangeInfoMetricsImpl"),
	}
}

const ExInfoNamespace = "binance_exchange_info"

func (s *exInfoMetrics) updateActiveMetrics() {
	if s.ReceivedExInfo == nil {
		s.ReceivedExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "received",
		})
		s.SentExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "sent",
		})
		s.SavedExInfo = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: ExInfoNamespace,
			Name:      "saved",
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

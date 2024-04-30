package metrics

import (
	"DeltaReceiver/internal/model"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/pkg/log"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type deltaMetrics struct {
	logger              *zap.Logger
	ReceivedDeltas      map[string]prometheus.Counter
	SentDeltas          map[string]prometheus.Counter
	SavedDeltas         map[string]prometheus.Counter
	ReceivedDeltasTotal prometheus.Counter
	SentDeltasTotal     prometheus.Counter
	SavedDeltasTotal    prometheus.Counter
}

func newDeltaMetrics() *deltaMetrics {
	return &deltaMetrics{
		logger:         log.GetLogger("DeltaMetricsImpl"),
		ReceivedDeltas: make(map[string]prometheus.Counter),
		SentDeltas:     make(map[string]prometheus.Counter),
		SavedDeltas:    make(map[string]prometheus.Counter),
	}
}

const BinanceDeltasNamespace = "binance_deltas"

func (s *deltaMetrics) updateActiveMetrics(symbols []string) {
	if s.ReceivedDeltasTotal == nil {
		s.ReceivedDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "received_total",
		})
		s.SentDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "sent_total",
		})
		s.SavedDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "saved_total",
		})
	}
	for _, symbol := range symbols {
		metricKey := getMetricKey(symbol)
		if _, ok := s.ReceivedDeltas[metricKey]; !ok {
			s.ReceivedDeltas[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("received_counter_%s", metricKey),
			})
			s.SentDeltas[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("sent_counter_%s", metricKey),
			})
			s.SavedDeltas[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("saved_counter_%s", metricKey),
			})
		}
	}
}

func (s *deltaMetrics) ProcessMetrics(deltas []model.Delta, event svc.TypeOfEvent) {
	if event == svc.Receive {
		s.processMetrics(deltas, s.ReceivedDeltas, s.ReceivedDeltasTotal)
	} else if event == svc.Send {
		s.processMetrics(deltas, s.SentDeltas, s.SentDeltasTotal)
	} else if event == svc.Save {
		s.processMetrics(deltas, s.SavedDeltas, s.SavedDeltasTotal)
	}
}

func (s *deltaMetrics) processMetrics(deltas []model.Delta, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
	for _, delta := range deltas {
		metricKey := getMetricKey(delta.Symbol)
		if metric, ok := metrics[metricKey]; !ok {
			s.logger.Warn(fmt.Sprintf("try to process non-existense metric with key [%s]", metricKey))
		} else {
			metric.Inc()
		}
	}
	totalMetric.Add(float64(len(deltas)))
}

package metrics

import (
	"DeltaReceiver/internal/model"
	"DeltaReceiver/internal/svc"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type deltaMetrics struct {
	ReceivedDeltas      map[string]prometheus.Counter
	SentDeltas          map[string]prometheus.Counter
	SavedDeltas         map[string]prometheus.Counter
	ReceivedDeltasTotal prometheus.Counter
	SentDeltasTotal     prometheus.Counter
	SavedDeltasTotal    prometheus.Counter
}

func newDeltaMetrics() *deltaMetrics {
	return &deltaMetrics{
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
			Name:      "received_deltas_total",
		})
		s.SentDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "sent_deltas_total",
		})
		s.SavedDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "saved_deltas_total",
		})
	}
	for _, symbol := range symbols {
		if _, ok := s.ReceivedDeltas[symbol]; !ok {
			s.ReceivedDeltas[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("received_deltas_counter_%s", symbol),
			})
			s.ReceivedDeltas[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("sent_deltas_counter_%s", symbol),
			})
			s.ReceivedDeltas[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceDeltasNamespace,
				Name:      fmt.Sprintf("saved_deltas_counter_%s", symbol),
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

func (_ *deltaMetrics) processMetrics(deltas []model.Delta, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
	for _, delta := range deltas {
		metrics[delta.Symbol].Inc()
	}
	totalMetric.Add(float64(len(deltas)))
}

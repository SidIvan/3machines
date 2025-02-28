package metrics

import (
	"DeltaReceiver/internal/nestor/svc"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const (
	nestorNamespace  = "nestor"
	binanceSubsystem = "binance"
)

type WsPipelineMetrics[T any] struct {
	logger                *zap.Logger
	startedSaveGoroutines prometheus.Counter
	endedSaveGoroutines   prometheus.Counter
	recvErrors            prometheus.Counter
	ReceivedDeltas        map[string]prometheus.Counter
	SentDeltas            map[string]prometheus.Counter
	SavedDeltas           map[string]prometheus.Counter
	ReceivedDeltasTotal   prometheus.Counter
	SentDeltasTotal       prometheus.Counter
	SavedDeltasTotal      prometheus.Counter
	RecvDeltaErr          prometheus.Counter
}

func NewWsPipelineMetrics[T any](dataType string) *WsPipelineMetrics[T] {
	return &WsPipelineMetrics[T]{
		startedSaveGoroutines: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: nestorNamespace,
			Subsystem: binanceSubsystem,
			Name:      fmt.Sprintf("started_save_%s_goroutines", dataType),
		}),
		endedSaveGoroutines: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: nestorNamespace,
			Subsystem: binanceSubsystem,
			Name:      fmt.Sprintf("ended_save_%s_goroutines", dataType),
		}),
		recvErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: nestorNamespace,
			Subsystem: binanceSubsystem,
			Name:      fmt.Sprintf("receive_%s_error", dataType),
		}),
	}
}

const BinanceDeltasNamespace = "binance_deltas"

func (s *WsPipelineMetrics[T]) IncStartedSaveGoroutines() {
	s.startedSaveGoroutines.Inc()
}

func (s *WsPipelineMetrics[T]) IncEndedSaveGoroutines() {
	s.endedSaveGoroutines.Inc()
}

func (s *WsPipelineMetrics[T]) IncRecvErr() {
	s.recvErrors.Inc()
}

// func (s *DeltaMetrics) updateActiveMetrics(symbols []string) {
// 	if s.ReceivedDeltasTotal == nil {
// 		s.ReceivedDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
// 			Namespace: BinanceDeltasNamespace,
// 			Subsystem: SpotSubsystem,
// 			Name:      "received_total",
// 		})
// 		s.SentDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
// 			Namespace: BinanceDeltasNamespace,
// 			Subsystem: SpotSubsystem,
// 			Name:      "sent_total",
// 		})
// 		s.SavedDeltasTotal = promauto.NewCounter(prometheus.CounterOpts{
// 			Namespace: BinanceDeltasNamespace,
// 			Subsystem: SpotSubsystem,
// 			Name:      "saved_total",
// 		})
// 	}
// 	for _, symbol := range symbols {
// 		metricKey := getMetricKey(symbol)
// 		if _, ok := s.ReceivedDeltas[metricKey]; !ok {
// 			s.ReceivedDeltas[symbol] = promauto.NewCounter(prometheus.CounterOpts{
// 				Namespace: BinanceDeltasNamespace,
// 				Subsystem: SpotSubsystem,
// 				Name:      fmt.Sprintf("received_%s", metricKey),
// 			})
// 			s.SentDeltas[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
// 				Namespace: BinanceDeltasNamespace,
// 				Subsystem: SpotSubsystem,
// 				Name:      fmt.Sprintf("sent_%s", metricKey),
// 			})
// 			s.SavedDeltas[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
// 				Namespace: BinanceDeltasNamespace,
// 				Subsystem: SpotSubsystem,
// 				Name:      fmt.Sprintf("saved_%s", metricKey),
// 			})
// 		}
// 	}
// }

func (s *WsPipelineMetrics[T]) ProcessDataMetrics(data []T, event svc.TypeOfEvent) {
	// if event == svc.Receive {
	// 	s.processMetrics(deltas, s.ReceivedDeltas, s.ReceivedDeltasTotal)
	// } else if event == svc.Send {
	// 	s.processMetrics(deltas, s.SentDeltas, s.SentDeltasTotal)
	// } else if event == svc.Save {
	// 	s.processMetrics(deltas, s.SavedDeltas, s.SavedDeltasTotal)
	// }
}

// func (s *DeltaMetrics) processMetrics(deltas []model.Delta, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
// 	for _, delta := range deltas {
// 		metricKey := getMetricKey(delta.Symbol)
// 		if metric, ok := metrics[metricKey]; !ok {
// 			s.logger.Warn(fmt.Sprintf("try to process non-existense metric with key [%s]", metricKey))
// 		} else {
// 			metric.Inc()
// 		}
// 	}
// 	totalMetric.Add(float64(len(deltas)))
// }

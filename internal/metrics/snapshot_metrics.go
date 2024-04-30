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

type snapshotMetrics struct {
	logger                 *zap.Logger
	ReceivedSnapshots      map[string]prometheus.Counter
	SentSnapshots          map[string]prometheus.Counter
	SavedSnapshots         map[string]prometheus.Counter
	ReceivedSnapshotsTotal prometheus.Counter
	SentSnapshotsTotal     prometheus.Counter
	SavedSnapshotsTotal    prometheus.Counter
}

func newSnapshotMetrics() *snapshotMetrics {
	return &snapshotMetrics{
		logger:            log.GetLogger("SnapshotMetricsImpl"),
		ReceivedSnapshots: make(map[string]prometheus.Counter),
		SentSnapshots:     make(map[string]prometheus.Counter),
		SavedSnapshots:    make(map[string]prometheus.Counter),
	}
}

const BinanceSnapshotsNamespace = "binance_snapshots"

func (s *snapshotMetrics) updateActiveMetrics(symbols []string) {
	if s.ReceivedSnapshotsTotal == nil {
		s.ReceivedSnapshotsTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceSnapshotsNamespace,
			Name:      "received_total",
		})
		s.SentSnapshotsTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceSnapshotsNamespace,
			Name:      "sent_total",
		})
		s.SavedSnapshotsTotal = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceSnapshotsNamespace,
			Name:      "saved_total",
		})
	}
	for _, symbol := range symbols {
		metricKey := getMetricKey(symbol)
		if _, ok := s.ReceivedSnapshots[metricKey]; !ok {
			s.ReceivedSnapshots[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceSnapshotsNamespace,
				Name:      fmt.Sprintf("received_%s", metricKey),
			})
			s.SentSnapshots[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceSnapshotsNamespace,
				Name:      fmt.Sprintf("sent_%s", metricKey),
			})
			s.SavedSnapshots[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceSnapshotsNamespace,
				Name:      fmt.Sprintf("saved_%s", metricKey),
			})
		}
	}
}

func (s *snapshotMetrics) ProcessMetrics(snapshot []model.DepthSnapshotPart, event svc.TypeOfEvent) {
	if event == svc.Receive {
		s.processMetrics(snapshot, s.ReceivedSnapshots, s.ReceivedSnapshotsTotal)
	} else if event == svc.Send {
		s.processMetrics(snapshot, s.SentSnapshots, s.SentSnapshotsTotal)
	} else if event == svc.Save {
		s.processMetrics(snapshot, s.SavedSnapshots, s.SavedSnapshotsTotal)
	}
}

func (s *snapshotMetrics) processMetrics(snapshot []model.DepthSnapshotPart, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
	if len(snapshot) == 0 {
		return
	}
	metricKey := getMetricKey(snapshot[0].Symbol)
	if metric, ok := metrics[metricKey]; !ok {
		s.logger.Warn(fmt.Sprintf("try to process non-existense metric with key [%s]", metricKey))
	} else {
		metric.Add(float64(len(snapshot)))
	}
	totalMetric.Inc()
}

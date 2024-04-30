package metrics

//
//import (
//	"DeltaReceiver/internal/svc"
//	bmodel "DeltaReceiver/pkg/binance/model"
//	"DeltaReceiver/pkg/log"
//	"fmt"
//	"github.com/prometheus/client_golang/prometheus"
//	"github.com/prometheus/client_golang/prometheus/promauto"
//	"go.uber.org/zap"
//)
//
//type snapshotMetrics struct {
//	logger                 *zap.Logger
//	ReceivedSnapshots      map[string]prometheus.Counter
//	SentSnapshots          map[string]prometheus.Counter
//	SavedSnapshots         map[string]prometheus.Counter
//	ReceivedSnapshotsTotal prometheus.Counter
//	SentSnapshotsTotal     prometheus.Counter
//	SaveSnapshotsTotal     prometheus.Counter
//}
//
//func newSnapshotMetrics() *snapshotMetrics {
//	return &snapshotMetrics{
//		logger:        log.GetLogger("SnapshotMetricsImpl"),
//		ReceivedSnapshots: make(map[string]prometheus.Counter),
//		SentSnapshots:     make(map[string]prometheus.Counter),
//		SavedSnapshots:    make(map[string]prometheus.Counter),
//	}
//}
//
//const BinanceSnapshotsNamespace = "binance_snapshots"
//
//func (s *snapshotMetrics) updateActiveMetrics(symbols []string) {
//	if s.ReceivedTicksTotal == nil {
//		s.ReceivedTicksTotal = promauto.NewCounter(prometheus.CounterOpts{
//			Namespace: BinanceTicksNamespace,
//			Name:      "received_ticks_total",
//		})
//		s.SentTicksTotal = promauto.NewCounter(prometheus.CounterOpts{
//			Namespace: BinanceTicksNamespace,
//			Name:      "sent_ticks_total",
//		})
//		s.SavedTicksTotal = promauto.NewCounter(prometheus.CounterOpts{
//			Namespace: BinanceTicksNamespace,
//			Name:      "saved_ticks_total",
//		})
//	}
//	for _, symbol := range symbols {
//		metricKey := getMetricKey(symbol)
//		if _, ok := s.ReceivedTicks[metricKey]; !ok {
//			s.ReceivedTicks[symbol] = promauto.NewCounter(prometheus.CounterOpts{
//				Namespace: BinanceTicksNamespace,
//				Name:      fmt.Sprintf("received_ticks_counter_%s", metricKey),
//			})
//			s.SentTicks[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
//				Namespace: BinanceTicksNamespace,
//				Name:      fmt.Sprintf("sent_ticks_counter_%s", metricKey),
//			})
//			s.SavedTicks[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
//				Namespace: BinanceTicksNamespace,
//				Name:      fmt.Sprintf("saved_ticks_counter_%s", metricKey),
//			})
//		}
//	}
//}
//
//func (s *ticksMetrics) ProcessMetrics(ticks []bmodel.SymbolTick, event svc.TypeOfEvent) {
//	if event == svc.Receive {
//		s.processMetrics(ticks, s.ReceivedTicks, s.ReceivedTicksTotal)
//	} else if event == svc.Send {
//		s.processMetrics(ticks, s.SentTicks, s.SentTicksTotal)
//	} else if event == svc.Save {
//		s.processMetrics(ticks, s.SavedTicks, s.SavedTicksTotal)
//	}
//}
//
//func (s *ticksMetrics) processMetrics(ticks []bmodel.SymbolTick, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
//	for _, tick := range ticks {
//		metricKey := getMetricKey(tick.Symbol)
//		if metric, ok := metrics[metricKey]; !ok {
//			s.logger.Warn(fmt.Sprintf("try to process non-existense metric with key [%s]", metricKey))
//		} else {
//			metric.Inc()
//		}
//	}
//	totalMetric.Add(float64(len(ticks)))
//}

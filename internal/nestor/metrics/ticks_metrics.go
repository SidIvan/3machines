package metrics

import (
	"DeltaReceiver/internal/nestor/svc"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type ticksMetrics struct {
	logger             *zap.Logger
	ReceivedTicks      map[string]prometheus.Counter
	SentTicks          map[string]prometheus.Counter
	SavedTicks         map[string]prometheus.Counter
	ReceivedTicksTotal prometheus.Counter
	SentTicksTotal     prometheus.Counter
	SavedTicksTotal    prometheus.Counter
	RecvTicksErr       prometheus.Counter
}

func newTicksMetrics() *ticksMetrics {
	return &ticksMetrics{
		logger:        log.GetLogger("ExInfoMetricsImpl"),
		ReceivedTicks: make(map[string]prometheus.Counter),
		SentTicks:     make(map[string]prometheus.Counter),
		SavedTicks:    make(map[string]prometheus.Counter),
		ReceivedTicksTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceTicksNamespace,
			Name:      "received_total",
		}),
		SentTicksTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceTicksNamespace,
			Name:      "sent_total",
		}),
		SavedTicksTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceTicksNamespace,
			Name:      "saved_total",
		}),
		RecvTicksErr: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: BinanceDeltasNamespace,
			Name:      "recieve_book_ticks_errors",
		}),
	}
}

const BinanceTicksNamespace = "binance_ticks"

func (s *ticksMetrics) updateActiveMetrics(symbols []string) {
	for _, symbol := range symbols {
		metricKey := getMetricKey(symbol)
		if _, ok := s.ReceivedTicks[metricKey]; !ok {
			s.ReceivedTicks[symbol] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceTicksNamespace,
				Name:      fmt.Sprintf("received_%s", metricKey),
			})
			s.SentTicks[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceTicksNamespace,
				Name:      fmt.Sprintf("sent_%s", metricKey),
			})
			s.SavedTicks[metricKey] = promauto.NewCounter(prometheus.CounterOpts{
				Namespace: BinanceTicksNamespace,
				Name:      fmt.Sprintf("saved_%s", metricKey),
			})
		}
	}
}

func (s *ticksMetrics) ProcessMetrics(ticks []bmodel.SymbolTick, event svc.TypeOfEvent) {
	if event == svc.Receive {
		s.processMetrics(ticks, s.ReceivedTicks, s.ReceivedTicksTotal)
	} else if event == svc.Send {
		s.processMetrics(ticks, s.SentTicks, s.SentTicksTotal)
	} else if event == svc.Save {
		s.processMetrics(ticks, s.SavedTicks, s.SavedTicksTotal)
	}
}

func (s *ticksMetrics) processMetrics(ticks []bmodel.SymbolTick, metrics map[string]prometheus.Counter, totalMetric prometheus.Counter) {
	for _, tick := range ticks {
		metricKey := getMetricKey(tick.Symbol)
		if metric, ok := metrics[metricKey]; !ok {
			s.logger.Warn(fmt.Sprintf("try to process non-existense metric with key [%s]", metricKey))
		} else {
			metric.Inc()
		}
	}
	totalMetric.Add(float64(len(ticks)))
}

func (s *ticksMetrics) IncTicksRecvErr() {
	s.RecvTicksErr.Inc()
}

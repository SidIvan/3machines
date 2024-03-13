package app

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/metrics"
	"DeltaReceiver/internal/repo"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/internal/web"
	"DeltaReceiver/pkg/log"
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"math"
	"net/http"
	"time"
)

type App struct {
	logger      *zap.Logger
	deltaRecSvc *svc.DeltaReceiverSvc
	cfg         *conf.AppConfig
}

func NewApp(cfg *conf.AppConfig) *App {
	globalRepo := repo.NewClickhouseRepo(cfg.GlobalRepoConfig)
	localRepo := repo.NewLocalMongoRepo(cfg.LocalRepoCfg)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig)
	metricsHolder := metrics.NewMetrics(cfg)
	var deltaReceivers []svc.DeltaReceiver
	for pair, period := range cfg.BinanceHttpConfig.Pair2Period {
		deltaReceivers = append(deltaReceivers, web.NewDeltaReceiverWs(cfg.BinanceHttpConfig, pair, period, cfg.ReconnectPeriodM))
	}
	validateSnapshotScheduling(cfg)
	exInfo := globalRepo.GetLastFullExchangeInfo(context.Background())
	if exInfo.Timezone == "" {
		var err error
		exInfo, err = binanceClient.GetFullExchangeInfo(context.Background())
		if err != nil {
			panic(err)
		}
		globalRepo.SendFullExchangeInfo(context.Background(), exInfo)
	}
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, deltaReceivers, localRepo, globalRepo, metricsHolder, exInfo)
	return &App{
		logger:      log.GetLogger("App"),
		deltaRecSvc: deltaRecSvc,
		cfg:         cfg,
	}
}

func validateSnapshotScheduling(cfg *conf.AppConfig) {
	numSnapsPerDay := 0.
	for _, period := range cfg.BinanceHttpConfig.SnapshotPeriod {
		numSnapsPerDay += math.Ceil(1440. / float64(period))
	}
	if numSnapsPerDay > 20 {
		panic("large amount of get full snapshot requests")
	}
}

func (s *App) Start() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	go s.deltaRecSvc.ReceiveDeltasPairs()
	for pair, period := range s.cfg.BinanceHttpConfig.SnapshotPeriod {
		go s.deltaRecSvc.CronGetAndStoreFullSnapshot(pair, period)
	}
	time.Sleep(1 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	s.deltaRecSvc.Shutdown(ctx)
	s.logger.Info("End of graceful shutdown")
}

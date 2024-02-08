package app

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/repo"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/internal/web"
	"DeltaReceiver/pkg/log"
	"context"
	"go.uber.org/zap"
	"math"
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
	var deltaReceivers []svc.DeltaReceiver
	for pair, period := range cfg.BinanceHttpConfig.Pair2Period {
		deltaReceivers = append(deltaReceivers, web.NewDeltaReceiverWs(cfg.BinanceHttpConfig, pair, period, cfg.ReconnectPeriodM))
	}
	validateSnapshotScheduling(cfg)
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, deltaReceivers, localRepo, globalRepo, nil)
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
	go s.deltaRecSvc.ReceiveDeltasPairs()
	//for pair, _ := range s.cfg.BinanceHttpConfig.Pair2Period {
	//	go s.deltaRecSvc.CronGetAndStoreFullSnapshot(pair, s.cfg.GetFullSnapshotPeriodM)
	//}
	time.Sleep(1 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	s.deltaRecSvc.Shutdown(ctx)
	s.logger.Info("End of graceful shutdown")
}

package app

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/repo"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/internal/web"
	"DeltaReceiver/pkg/log"
	"context"
	"go.uber.org/zap"
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
		deltaReceivers = append(deltaReceivers, *web.NewDeltaReceiverWs(cfg.BinanceHttpConfig, pair, period))
	}
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, deltaReceivers, localRepo, globalRepo, nil)
	return &App{
		logger:      log.GetLogger("App"),
		deltaRecSvc: deltaRecSvc,
		cfg:         cfg,
	}
}

func (s *App) Start() {
	go s.deltaRecSvc.ReceiveDeltasPairs()
	for pair, _ := range s.cfg.BinanceHttpConfig.Pair2Period {
		go s.deltaRecSvc.CronGetAndStoreFullSnapshot(pair, s.cfg.GetFullSnapshotPeriodM)
	}
	time.Sleep(1 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.deltaRecSvc.Shutdown(ctx)
}

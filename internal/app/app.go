package app

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/metrics"
	"DeltaReceiver/internal/repo"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/internal/web"
	"DeltaReceiver/pkg/log"
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type App struct {
	logger        *zap.Logger
	deltaRecSvc   *svc.DeltaReceiverSvc
	snapshotSvc   *svc.SnapshotSvc
	exInfoSvc     *svc.ExchangeInfoSvc
	bookTickerSvc *svc.BookTickerSvc
	globalRepo    svc.GlobalRepo
	localRepo     svc.LocalRepo
	exInfoCache   *cache.ExchangeInfoCache
	binanceClient svc.BinanceClient
	cfg           *conf.AppConfig
}

func NewApp(cfg *conf.AppConfig) *App {
	logger := log.GetLogger("App")
	exInfoCache := cache.NewExchangeInfoCache()
	globalRepo := repo.NewClickhouseRepo(cfg.GlobalRepoConfig)
	localRepo := repo.NewLocalMongoRepo(cfg.LocalRepoCfg)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig, exInfoCache)
	metricsHolder := metrics.NewMetrics(cfg)
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	snapshotSvc := svc.NewSnapshotSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	exInfoSvc := svc.NewExchangeInfoSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	bookTickerSvc := svc.NewBookerTickSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder)
	return &App{
		logger:        logger,
		deltaRecSvc:   deltaRecSvc,
		snapshotSvc:   snapshotSvc,
		exInfoSvc:     exInfoSvc,
		bookTickerSvc: bookTickerSvc,
		globalRepo:    globalRepo,
		localRepo:     localRepo,
		exInfoCache:   exInfoCache,
		binanceClient: binanceClient,
		cfg:           cfg,
	}
}

func (s *App) Start() {
	baseContext := context.Background()
	if err := s.globalRepo.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
		return
	}
	if err := s.localRepo.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
		return
	}
	exInfo := s.globalRepo.GetLastFullExchangeInfo(context.Background())
	if exInfo.Timezone == "" {
		var err error
		exInfo, err = s.binanceClient.GetFullExchangeInfo(context.Background())
		if err != nil {
			s.logger.Error(err.Error())
			return
		}
		if err = s.globalRepo.SendFullExchangeInfo(baseContext, exInfo); err != nil {
			s.logger.Error(err.Error())
			return
		}
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	ctx := context.Background()
	go s.deltaRecSvc.ReceiveDeltasPairs(ctx)
	go s.snapshotSvc.StartReceiveAndSaveSnapshots(ctx)
	go s.exInfoSvc.StartReceiveExInfo(ctx)
	time.Sleep(1 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	go s.deltaRecSvc.Shutdown(ctx)
	go s.snapshotSvc.Shutdown(ctx)
	go s.exInfoSvc.Shutdown(ctx)
	go s.bookTickerSvc.Shutdown(ctx)
	s.logger.Info("End of graceful shutdown")
}

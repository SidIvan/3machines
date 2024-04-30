package app

import (
	"DeltaReceiver/internal/cache"
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/metrics"
	"DeltaReceiver/internal/repo"
	"DeltaReceiver/internal/svc"
	"DeltaReceiver/internal/web"
	"DeltaReceiver/pkg/binance"
	"DeltaReceiver/pkg/log"
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"sync"
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
	metrics       svc.MetricsHolder
	cfg           *conf.AppConfig
}

func NewApp(cfg *conf.AppConfig) *App {
	binance.InitLogger()
	logger := log.GetLogger("App")
	metricsHolder := metrics.NewMetrics(cfg)
	exInfoCache := cache.NewExchangeInfoCache()
	globalRepo := repo.NewClickhouseRepo(cfg.GlobalRepoConfig)
	localRepo := repo.NewLocalMongoRepo(cfg.LocalRepoCfg)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig, exInfoCache)
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	snapshotSvc := svc.NewSnapshotSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	exInfoSvc := svc.NewExchangeInfoSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	bookTickerSvc := svc.NewBookTickerSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
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
		metrics:       metricsHolder,
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
	exInfo, err := s.binanceClient.GetFullExchangeInfo(context.Background())
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	if err = s.globalRepo.SendFullExchangeInfo(baseContext, exInfo); err != nil {
		s.logger.Error(err.Error())
		return
	}
	var symbols []string
	for _, symbol := range exInfo.Symbols {
		symbols = append(symbols, symbol.Symbol)
	}
	s.metrics.UpdateMetrics(exInfo.Symbols)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	s.logger.Info("App started")
	go s.deltaRecSvc.ReceiveDeltasPairs(baseContext)
	go s.snapshotSvc.StartReceiveAndSaveSnapshots(baseContext)
	go s.exInfoSvc.StartReceiveExInfo(baseContext)
	go s.bookTickerSvc.StartReceiveOrderBooksTops(baseContext)
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		s.deltaRecSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.snapshotSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.exInfoSvc.Shutdown(ctx)
		//wg.Done()
	}()
	go func() {
		s.bookTickerSvc.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
	s.globalRepo.Disconnect(context.Background())
	s.logger.Info("End of graceful shutdown")
}

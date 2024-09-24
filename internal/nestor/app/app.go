package app

import (
	crepo "DeltaReceiver/internal/common/repo"
	csvc "DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/internal/nestor/metrics"
	"DeltaReceiver/internal/nestor/repo"
	"DeltaReceiver/internal/nestor/svc"
	"DeltaReceiver/internal/nestor/web"
	"DeltaReceiver/pkg/binance"
	"DeltaReceiver/pkg/clickhouse"
	"DeltaReceiver/pkg/log"
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type App struct {
	logger              *zap.Logger
	deltaRecSvc         *svc.DeltaReceiverSvc
	snapshotSvc         *svc.SnapshotSvc
	exInfoSvc           *svc.ExchangeInfoSvc
	bookTickerSvc       *svc.BookTickerSvc
	globalRepo          svc.GlobalRepo
	deltaStorage        csvc.DeltaStorage
	localRepo           svc.LocalRepo
	exInfoCache         *cache.ExchangeInfoCache
	binanceClient       svc.BinanceClient
	metrics             svc.MetricsHolder
	deltaFixer          svc.Fixer
	deltaHolesIdWatcher *cache.DeltaUpdateIdWatcher
	deltaHolesStorage   svc.DeltaHolesStorage
	cfg                 *conf.AppConfig
}

func NewApp(cfg *conf.AppConfig) *App {
	log.InitServiceName("nestor")
	binance.InitLogger()
	logger := log.GetLogger("App")
	metricsHolder := metrics.NewMetrics()
	exInfoCache := cache.NewExchangeInfoCache()
	deltaHolesStorage := repo.NewDwarfHttpClient(cfg.DwarfUrl)
	deltaHolesIdWatcher := cache.NewDeltaUpdateIdWatcher()
	chPoolHolder := clickhouse.NewChPoolHolder(cfg.GlobalRepoConfig.ChPoolCfg)
	globalRepo := repo.NewClickhouseRepo(chPoolHolder, cfg.GlobalRepoConfig)
	deltaStorage := crepo.NewChDeltaStorage(chPoolHolder, cfg.GlobalRepoConfig.DatabaseName, cfg.GlobalRepoConfig.DeltaTable)
	localRepo := repo.NewLocalMongoRepo(cfg.LocalRepoCfg)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig, exInfoCache)
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, localRepo, globalRepo, deltaStorage, metricsHolder, exInfoCache, deltaHolesIdWatcher, deltaHolesStorage)
	snapshotSvc := svc.NewSnapshotSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	exInfoSvc := svc.NewExchangeInfoSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	bookTickerSvc := svc.NewBookTickerSvc(cfg, binanceClient, localRepo, globalRepo, metricsHolder, exInfoCache)
	deltaFixer := svc.NewDeltaFixer(cfg, deltaStorage, localRepo)
	return &App{
		logger:              logger,
		deltaRecSvc:         deltaRecSvc,
		snapshotSvc:         snapshotSvc,
		exInfoSvc:           exInfoSvc,
		bookTickerSvc:       bookTickerSvc,
		globalRepo:          globalRepo,
		deltaStorage:        deltaStorage,
		localRepo:           localRepo,
		exInfoCache:         exInfoCache,
		binanceClient:       binanceClient,
		metrics:             metricsHolder,
		deltaFixer:          deltaFixer,
		deltaHolesIdWatcher: deltaHolesIdWatcher,
		deltaHolesStorage:   deltaHolesStorage,
		cfg:                 cfg,
	}
}

func (s *App) Start() {
	baseContext := context.Background()
	if err := s.globalRepo.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
	}
	if err := s.localRepo.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
	}
	exInfo, err := s.binanceClient.GetFullExchangeInfo(context.Background())
	if err != nil {
		s.logger.Error(err.Error())
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(2 * time.Second)
	if err = s.globalRepo.SendFullExchangeInfo(baseContext, exInfo); err != nil {
		s.logger.Error(err.Error())
	}
	var symbols []string
	for _, symbol := range exInfo.Symbols {
		symbols = append(symbols, symbol.Symbol)
	}
	s.metrics.UpdateMetrics(exInfo.Symbols)
	systemMonitorer := metrics.NewSystemMonitorer(s.metrics)
	s.runFixers()
	go systemMonitorer.CronUpdateMetrics()
	s.logger.Info("App started")
	go s.deltaRecSvc.ReceiveDeltasPairs(baseContext)
	go s.snapshotSvc.StartReceiveAndSaveSnapshots(baseContext)
	go s.exInfoSvc.StartReceiveExInfo(baseContext)
	go s.bookTickerSvc.StartReceiveOrderBooksTops(baseContext)
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		s.deltaRecSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.snapshotSvc.Shutdown(ctx)
		//wg.Done()
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

func (s *App) runFixers() {
	go s.deltaFixer.Fix()
}

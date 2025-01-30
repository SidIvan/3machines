package app

import (
	cconf "DeltaReceiver/internal/common/conf"
	"DeltaReceiver/internal/common/repo/cs"
	csvc "DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/internal/nestor/metrics"
	"DeltaReceiver/internal/nestor/repo"
	"DeltaReceiver/internal/nestor/svc"
	"DeltaReceiver/internal/nestor/web"
	"DeltaReceiver/pkg/binance"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type App struct {
	logger              *zap.Logger
	deltaRecSvc         *svc.DeltaReceiverSvc
	snapshotSvc         *svc.SnapshotSvc
	exInfoSvc           *svc.ExchangeInfoSvc
	bookTickerSvc       *svc.BookTickerSvc
	deltaStorage        csvc.DeltaStorage
	snapshotStorage     csvc.SnapshotStorage
	bookTicksStorage    csvc.BookTicksStorage
	exchangeInfoStorage csvc.ExchangeInfoStorage
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
	rawCfg, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(rawCfg))
	binance.InitLogger()
	logger := log.GetLogger("App")
	metricsHolder := metrics.NewMetrics()
	exInfoCache := cache.NewExchangeInfoCache()
	deltaHolesStorage := repo.NewDwarfHttpClient(cfg.DwarfUrl)
	deltaHolesIdWatcher := cache.NewDeltaUpdateIdWatcher()
	csCfg := cfg.CsCfg
	csSession := initCs(csCfg)
	deltaStorage := cs.NewCsDeltaStorage(csSession, csCfg.DeltaTableName, csCfg.DeltaKeyTableName)
	snapshotStorage := cs.NewCsSnapshotStorage(csSession, csCfg.SnapshotTableName)
	bookTicksStorage := cs.NewCsBookTicksStorage(csSession, csCfg.BookTicksTableName)
	exchangeInfoStorage := cs.NewExchangeInfoStorage(csSession, csCfg.ExchangeInfoTableName)
	localRepo := repo.NewLocalMongoRepo(cfg.LocalRepoCfg)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig, exInfoCache)
	deltaRecSvc := svc.NewDeltaReceiverSvc(cfg, binanceClient, localRepo, deltaStorage, metricsHolder, exInfoCache, deltaHolesIdWatcher, deltaHolesStorage)
	snapshotSvc := svc.NewSnapshotSvc(cfg, binanceClient, localRepo, snapshotStorage, metricsHolder, exInfoCache)
	exInfoSvc := svc.NewExchangeInfoSvc(cfg, binanceClient, localRepo, exchangeInfoStorage, metricsHolder, exInfoCache)
	bookTickerSvc := svc.NewBookTickerSvc(cfg, binanceClient, localRepo, bookTicksStorage, metricsHolder, exInfoCache)
	deltaFixer := svc.NewDeltaFixer(cfg, deltaStorage, localRepo)
	return &App{
		logger:              logger,
		deltaRecSvc:         deltaRecSvc,
		snapshotSvc:         snapshotSvc,
		exInfoSvc:           exInfoSvc,
		bookTickerSvc:       bookTickerSvc,
		deltaStorage:        deltaStorage,
		bookTicksStorage:    bookTicksStorage,
		exchangeInfoStorage: exchangeInfoStorage,
		snapshotStorage:     snapshotStorage,
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

func initCs(cfg *cconf.CsRepoConfig) *gocql.Session {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Keyspace = cfg.KeySpace
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	return session
}

func (s *App) Start() {
	baseContext := context.Background()
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
	if err = s.exchangeInfoStorage.SendExchangeInfo(baseContext, exInfo); err != nil {
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
	time.Sleep(30 * time.Second)
	s.deltaStorage.Disconnect(context.Background())
	s.logger.Info("End of graceful shutdown")
}

func (s *App) runFixers() {
	go s.deltaFixer.Fix()
}

package app

import (
	cconf "DeltaReceiver/internal/common/conf"
	cm "DeltaReceiver/internal/common/metrics"
	cmodel "DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	cweb "DeltaReceiver/internal/common/web"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/internal/nestor/metrics"
	"DeltaReceiver/internal/nestor/model"
	"DeltaReceiver/internal/nestor/repo"
	"DeltaReceiver/internal/nestor/svc"
	"DeltaReceiver/internal/nestor/web"
	"DeltaReceiver/pkg/binance"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type App struct {
	logger              *zap.Logger
	deltaSvc            *svc.WsSvc[bmodel.DeltaMessage, cmodel.Delta]
	ticksSvc            *svc.WsSvc[bmodel.SymbolTick, bmodel.SymbolTick]
	snapshotSvc         *svc.SnapshotSvc
	exInfoSvc           *svc.ExchangeInfoSvc
	exchangeInfoStorage svc.ExchangeInfoStorage
	exInfoCache         *cache.ExchangeInfoCache
	binanceClient       svc.BinanceClient
	metrics             svc.MetricsHolder
	deltaFixer          svc.Fixer
	ticksFixer          svc.Fixer
	snapshotFixer       svc.Fixer
	exInfoFixer         svc.Fixer
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
	deltaHolesStorage := cweb.NewDwarfHttpClient(cfg.DwarfUrl)
	deltaHolesIdWatcher := cache.NewDeltaUpdateIdWatcher()
	csCfg := cfg.CsCfg
	csSession := initCs(csCfg)
	reconnectPeriod := time.Minute * time.Duration(cfg.ReconnectPeriodM)
	binanceClient := web.NewBinanceClient(cfg.BinanceHttpConfig, exInfoCache)

	// binance spot deltas
	deltaCsStorage := cs.NewCsDeltaStorageWO(csSession, cm.NewCsStorageMetrics(csCfg.DeltaTableName), csCfg.DeltaTableName, csCfg.DeltaKeyTableName)
	deltaFileStorage := repo.NewFileRepo[cmodel.Delta]("deltas_spot")
	deltaStorages := []svc.BatchedDataStorage[cmodel.Delta]{deltaCsStorage, deltaFileStorage}
	deltasTransformator := model.NewDeltaDataTransformator()
	deltasMetrics := metrics.NewWsPipelineMetrics[cmodel.Delta]("deltas_spot")
	deltaWorkerProvider := svc.NewDeltaWorkerProvider(cfg.BinanceHttpConfig, "deltas_spot", deltasTransformator, cfg.BinanceSpotDeltasPipeline.BatchSize, deltaStorages, deltasMetrics)
	deltaWorkersProvider := svc.NewTradingSymbolsWorkersProvider("deltas_spot", cfg.BinanceSpotDeltasPipeline.NumWorkers, deltaWorkerProvider, exInfoCache)
	deltaSvc := svc.NewWsSvc("deltas_spot", deltaWorkersProvider, deltaStorages, deltasMetrics, reconnectPeriod, exInfoCache)
	deltaFixer := svc.NewDataFixer("deltas_spot", deltaCsStorage, []svc.AuxBatchedDataStorage[cmodel.Delta]{deltaFileStorage})

	// binance spot book ticks
	ticksCsStorage := cs.NewCsBookTicksStorageWO(csSession, cm.NewCsStorageMetrics(csCfg.BookTicksTableName), csCfg.BookTicksTableName, csCfg.BookTicksKeyTableName)
	ticksFileStorage := repo.NewFileRepo[bmodel.SymbolTick]("book_ticker_spot")
	ticksStorages := []svc.BatchedDataStorage[bmodel.SymbolTick]{ticksCsStorage, ticksFileStorage}
	ticksTransformator := model.NewNoChangeTransformator[bmodel.SymbolTick]()
	ticksMetrics := metrics.NewWsPipelineMetrics[bmodel.SymbolTick]("book_ticker_spot")
	ticksWorkerProvider := svc.NewBookTicksWorkerProvider(cfg.BinanceHttpConfig, "book_ticker_spot", ticksTransformator, cfg.BinanceSpotBookTicksPipeline.BatchSize, ticksStorages, ticksMetrics)
	ticksWorkersProvider := svc.NewTradingSymbolsWorkersProvider("book_ticker_spot", cfg.BinanceSpotDeltasPipeline.NumWorkers, ticksWorkerProvider, exInfoCache)
	ticksSvc := svc.NewWsSvc("book_ticker_spot", ticksWorkersProvider, ticksStorages, ticksMetrics, reconnectPeriod, exInfoCache)
	ticksFixer := svc.NewDataFixer("book_ticker_spot", ticksCsStorage, []svc.AuxBatchedDataStorage[bmodel.SymbolTick]{ticksFileStorage})

	// binance spot depth snapshots
	snapshotCsStorage := cs.NewCsSnapshotStorageWO(csSession, cm.NewCsStorageMetrics(csCfg.SnapshotTableName), csCfg.SnapshotTableName, csCfg.SnapshotKeyTableName)
	snapshotFileStorage := repo.NewFileRepo[cmodel.DepthSnapshotPart]("snapshots_spot")
	snapshotStorages := []svc.BatchedDataStorage[cmodel.DepthSnapshotPart]{snapshotCsStorage, snapshotFileStorage}
	snapshotSvc := svc.NewSnapshotSvc(cfg, binanceClient, snapshotStorages, metricsHolder, exInfoCache)
	snapshotFixer := svc.NewDataFixer("snapshot_spot", snapshotCsStorage, []svc.AuxBatchedDataStorage[cmodel.DepthSnapshotPart]{snapshotFileStorage})

	// binance spot exchange info
	exchangeInfoCsStorage := cs.NewExchangeInfoStorage(csSession, csCfg.ExchangeInfoTableName)
	exchangeInfoFileStorage := repo.NewFileRepo[cmodel.ExchangeInfo]("exchange_info_spot")
	exInfoStorages := []svc.BatchedDataStorage[cmodel.ExchangeInfo]{exchangeInfoCsStorage, exchangeInfoFileStorage}
	exInfoSvc := svc.NewExchangeInfoSvc(cfg, binanceClient, exInfoStorages, metricsHolder, exInfoCache)
	exInfoFixer := svc.NewDataFixer("exchange_info_spot", exchangeInfoCsStorage, []svc.AuxBatchedDataStorage[cmodel.ExchangeInfo]{exchangeInfoFileStorage})
	return &App{
		logger:              logger,
		deltaSvc:            deltaSvc,
		ticksSvc:            ticksSvc,
		snapshotSvc:         snapshotSvc,
		exInfoSvc:           exInfoSvc,
		exInfoCache:         exInfoCache,
		binanceClient:       binanceClient,
		exchangeInfoStorage: exchangeInfoCsStorage,
		metrics:             metricsHolder,
		deltaFixer:          deltaFixer,
		ticksFixer:          ticksFixer,
		snapshotFixer:       snapshotFixer,
		exInfoFixer:         exInfoFixer,
		deltaHolesIdWatcher: deltaHolesIdWatcher,
		deltaHolesStorage:   deltaHolesStorage,
		// mongoClient:         mongoClient,
		cfg: cfg,
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

func initMongo(cfg *conf.LocalRepoConfig) *mongo.Client {
	ctx := context.TODO()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(cfg.MongoConfig.TimeoutS)*time.Second)
	client, err := mongo.Connect(ctxWithTimeout, options.Client().ApplyURI(cfg.MongoConfig.URI.GetBaseUri()).SetDirect(true))
	cancel()
	if err != nil {
		panic(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.MongoConfig.TimeoutS)*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func (s *App) Start() {
	baseContext := context.Background()
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
	if err = s.exchangeInfoStorage.SendExchangeInfo(baseContext, cmodel.NewExchangeInfo(exInfo)); err != nil {
		s.logger.Error(err.Error())
	}
	s.metrics.UpdateMetrics(exInfo.Symbols)
	s.runFixers()
	s.logger.Info("App started")
	go s.deltaSvc.Start(baseContext)
	go s.ticksSvc.Start(baseContext)
	go s.snapshotSvc.StartReceiveAndSaveSnapshots(baseContext)
	go s.exInfoSvc.StartReceiveExInfo(baseContext)
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		s.deltaSvc.Shutdown(ctx)
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
		s.ticksSvc.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
	time.Sleep(30 * time.Second)
	// s.mongoClient.Disconnect(context.Background())
	s.logger.Info("End of graceful shutdown")
}

func (s *App) runFixers() {
	go s.deltaFixer.Fix()
	go s.ticksFixer.Fix()
	go s.snapshotFixer.Fix()
	go s.exInfoFixer.Fix()
}

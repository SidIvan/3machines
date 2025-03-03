package app

import (
	cconf "DeltaReceiver/internal/common/conf"
	cm "DeltaReceiver/internal/common/metrics"
	cmodel "DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	"DeltaReceiver/internal/nestor/cache"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/internal/nestor/metrics"
	"DeltaReceiver/internal/nestor/model"
	"DeltaReceiver/internal/nestor/repo"
	"DeltaReceiver/internal/nestor/svc"
	"DeltaReceiver/internal/nestor/web"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type BinanceMarketCtx struct {
	logger              *zap.Logger
	deltaSvc            *svc.WsSvc[bmodel.DeltaMessage, cmodel.Delta]
	ticksSvc            *svc.WsSvc[bmodel.SymbolTick, bmodel.SymbolTick]
	snapshotSvc         *svc.SnapshotSvc
	exInfoSvc           *svc.ExchangeInfoSvc
	exchangeInfoStorage svc.ExchangeInfoStorage
	exInfoCache         *cache.ExchangeInfoCache
	binanceClient       svc.BinanceClient
	deltaFixer          svc.Fixer
	ticksFixer          svc.Fixer
	snapshotFixer       svc.Fixer
	exInfoFixer         svc.Fixer
}

func NewBinanceMarketCtx(
	marketCfg *conf.BinanceMarketCfg,
	marketCsRepoCfg *cconf.BinanceMarketCsRepoCfg,
	csSession *gocql.Session,
	binanceReconnectPeriod time.Duration,
) *BinanceMarketCtx {
	marketType := bmodel.DataType(marketCfg.DataType)
	exInfoCache := cache.NewExchangeInfoCache()
	binanceClient := web.NewBinanceClient(marketType, marketCfg.BinanceHttpCfg, exInfoCache)

	// deltas
	loggerParam := string("deltas_" + marketType)
	deltaCsStorage := cs.NewCsDeltaStorageWO(csSession, cm.NewCsStorageMetrics(marketCsRepoCfg.DeltaTableName, marketType), marketCsRepoCfg.DeltaTableName, marketCsRepoCfg.DeltaKeyTableName)
	deltaFileStorage := repo.NewFileRepo[cmodel.Delta](loggerParam)
	deltaStorages := []svc.BatchedDataStorage[cmodel.Delta]{deltaCsStorage, deltaFileStorage}
	deltasTransformator := model.NewDeltaDataTransformator()
	deltasMetrics := metrics.NewWsPipelineMetrics[cmodel.Delta](loggerParam)
	deltaWorkerProvider := svc.NewDeltaWorkerProvider(marketCfg.BinanceHttpCfg, loggerParam, marketType, deltasTransformator, marketCfg.DeltasPipelineCfg.BatchSize, deltaStorages, deltasMetrics)
	deltaWorkersProvider := svc.NewTradingSymbolsWorkersProvider(loggerParam, marketCfg.DeltasPipelineCfg.NumWorkers, deltaWorkerProvider, exInfoCache)
	deltaSvc := svc.NewWsSvc(loggerParam, deltaWorkersProvider, deltaStorages, deltasMetrics, binanceReconnectPeriod, exInfoCache)
	deltaFixer := svc.NewDataFixer(loggerParam, deltaCsStorage, []svc.AuxBatchedDataStorage[cmodel.Delta]{deltaFileStorage})

	// book ticks
	loggerParam = string("book_ticks_" + marketType)
	ticksCsStorage := cs.NewCsBookTicksStorageWO(csSession, cm.NewCsStorageMetrics(marketCsRepoCfg.BookTicksTableName, marketType), marketCsRepoCfg.BookTicksTableName, marketCsRepoCfg.BookTicksKeyTableName)
	ticksFileStorage := repo.NewFileRepo[bmodel.SymbolTick](loggerParam)
	ticksStorages := []svc.BatchedDataStorage[bmodel.SymbolTick]{ticksCsStorage, ticksFileStorage}
	ticksTransformator := model.NewNoChangeTransformator[bmodel.SymbolTick]()
	ticksMetrics := metrics.NewWsPipelineMetrics[bmodel.SymbolTick](loggerParam)
	var ticksWorkersProvider svc.WsDataWorkersProvider[svc.WsDataProcessWorker[bmodel.SymbolTick, bmodel.SymbolTick]]
	if !marketCfg.BinanceHttpCfg.UseAllTickersStream {
		ticksWorkerProvider := svc.NewBookTicksWorkerProvider(marketCfg.BinanceHttpCfg, loggerParam, marketType, ticksTransformator, marketCfg.BookTicksPipelineCfg.BatchSize, ticksStorages, ticksMetrics)
		ticksWorkersProvider = svc.NewTradingSymbolsWorkersProvider(loggerParam, marketCfg.BookTicksPipelineCfg.NumWorkers, ticksWorkerProvider, exInfoCache)
	} else {
		ticksWorkersProvider = svc.NewBookTicksAllStreamsWorkerProvider(marketCfg.BinanceHttpCfg, loggerParam, ticksTransformator, marketCfg.BookTicksPipelineCfg.BatchSize, ticksStorages, ticksMetrics)
	}
	ticksSvc := svc.NewWsSvc(loggerParam, ticksWorkersProvider, ticksStorages, ticksMetrics, binanceReconnectPeriod, exInfoCache)
	ticksFixer := svc.NewDataFixer(loggerParam, ticksCsStorage, []svc.AuxBatchedDataStorage[bmodel.SymbolTick]{ticksFileStorage})

	// depth snapshots
	loggerParam = string("snapshots_" + marketType)
	snapshotCsStorage := cs.NewCsSnapshotStorageWO(csSession, cm.NewCsStorageMetrics(marketCsRepoCfg.SnapshotTableName, marketType), marketCsRepoCfg.SnapshotTableName, marketCsRepoCfg.SnapshotKeyTableName)
	snapshotFileStorage := repo.NewFileRepo[cmodel.DepthSnapshotPart](loggerParam)
	snapshotStorages := []svc.BatchedDataStorage[cmodel.DepthSnapshotPart]{snapshotCsStorage, snapshotFileStorage}
	snapshotSvc := svc.NewSnapshotSvc(binanceClient, snapshotStorages, exInfoCache)
	snapshotFixer := svc.NewDataFixer("snapshot_spot", snapshotCsStorage, []svc.AuxBatchedDataStorage[cmodel.DepthSnapshotPart]{snapshotFileStorage})

	// binance spot exchange info
	loggerParam = string("exchange_info_" + marketType)
	exchangeInfoCsStorage := cs.NewExchangeInfoStorage(csSession, marketCsRepoCfg.ExchangeInfoTableName)
	exchangeInfoFileStorage := repo.NewFileRepo[cmodel.ExchangeInfo](loggerParam)
	exInfoStorages := []svc.BatchedDataStorage[cmodel.ExchangeInfo]{exchangeInfoCsStorage, exchangeInfoFileStorage}
	exInfoSvc := svc.NewExchangeInfoSvc(time.Duration(marketCfg.ExchangeInfoUpdPerM)*time.Minute, binanceClient, exInfoStorages, exInfoCache)
	exInfoFixer := svc.NewDataFixer(loggerParam, exchangeInfoCsStorage, []svc.AuxBatchedDataStorage[cmodel.ExchangeInfo]{exchangeInfoFileStorage})

	return &BinanceMarketCtx{
		logger:              log.GetLogger(fmt.Sprintf("BinanceMarketCtx[%s]", marketType)),
		deltaSvc:            deltaSvc,
		ticksSvc:            ticksSvc,
		snapshotSvc:         snapshotSvc,
		exInfoSvc:           exInfoSvc,
		exchangeInfoStorage: exchangeInfoCsStorage,
		exInfoCache:         exInfoCache,
		binanceClient:       binanceClient,
		deltaFixer:          deltaFixer,
		ticksFixer:          ticksFixer,
		snapshotFixer:       snapshotFixer,
		exInfoFixer:         exInfoFixer,
	}
}

func (s *BinanceMarketCtx) Start(ctx context.Context) {
	exInfo, err := s.binanceClient.GetFullExchangeInfo(context.Background())
	if err != nil {
		s.logger.Error(err.Error())
	}
	if err = s.exchangeInfoStorage.SendExchangeInfo(ctx, cmodel.NewExchangeInfo(exInfo)); err != nil {
		s.logger.Error(err.Error())
	}
	go s.deltaSvc.Start(ctx)
	go s.ticksSvc.Start(ctx)
	go s.snapshotSvc.StartReceiveAndSaveSnapshots(ctx)
	go s.exInfoSvc.StartReceiveExInfo(ctx)
	go s.deltaFixer.Fix()
	go s.ticksFixer.Fix()
	go s.snapshotFixer.Fix()
	go s.exInfoFixer.Fix()
}

func (s *BinanceMarketCtx) Shutdown(ctx context.Context) {
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
	s.logger.Info("End of graceful shutdown")
}

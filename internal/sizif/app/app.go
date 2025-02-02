package app

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	b2pqt "DeltaReceiver/internal/sizif/b2"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/internal/sizif/lock"
	"DeltaReceiver/internal/sizif/svc"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Backblaze/blazer/b2"
	"github.com/go-zookeeper/zk"
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type App struct {
	logger       *zap.Logger
	cfg          *conf.AppConfig
	deltasSvc    *svc.SizifSvc[model.Delta]
	bookTicksSvc *svc.SizifSvc[bmodel.SymbolTick]
	snapshotsSvc *svc.SizifSvc[model.DepthSnapshotPart]
}

func NewApp(cfg *conf.AppConfig) *App {
	log.InitServiceName("sizif")
	logger := log.GetLogger("App")
	rawCfg, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(rawCfg))
	zkConn, b2Bucket, csSession := initConnections(cfg)

	deltaSocratesStorage := cs.NewCsDeltaStorage(csSession, cfg.SocratesCfg.DeltaTableName, cfg.SocratesCfg.DeltaKeyTableName)
	deltaParquetStorage := b2pqt.NewB2ParquetStorage[model.Delta](b2Bucket, "binance/deltas")
	deltaTransformator := svc.NewDeltaTransformator()
	deltaLocker := lock.NewZkLocker("binance/deltas", zkConn)
	deltaSvc := svc.NewSizifSvc("binance/deltas", deltaSocratesStorage, deltaParquetStorage, deltaTransformator, deltaLocker, cfg.DeltaWorkers)

	bookTicksSocratesStorage := cs.NewCsBookTicksStorage(csSession, cfg.SocratesCfg.BookTicksTableName, cfg.SocratesCfg.BookTicksKeyTableName)
	bookTicksParquetStorage := b2pqt.NewB2ParquetStorage[bmodel.SymbolTick](b2Bucket, "binance/book_ticks")
	bookTicksTransformator := svc.NewBookTicksTransformator()
	bookTicksLocker := lock.NewZkLocker("binance/book_ticks", zkConn)
	bookTicksSvc := svc.NewSizifSvc("binance/book_ticks", bookTicksSocratesStorage, bookTicksParquetStorage, bookTicksTransformator, bookTicksLocker, cfg.BookTicksWorker)

	snapshotsSocratesStorage := cs.NewCsSnapshotStorage(csSession, cfg.SocratesCfg.SnapshotTableName, cfg.SocratesCfg.SnapshotKeyTableName)
	snapshotsParquetStorage := b2pqt.NewB2ParquetStorage[model.DepthSnapshotPart](b2Bucket, "binance/snapshots")
	snapshotsTransformator := svc.NewDepthSnapshotTransformator()
	snapshotsLocker := lock.NewZkLocker("binance/snapshots", zkConn)
	snapshotsSvc := svc.NewSizifSvc("binance/snapshots", snapshotsSocratesStorage, snapshotsParquetStorage, snapshotsTransformator, snapshotsLocker, cfg.SnapshotsWorker)
	return &App{
		logger:       logger,
		cfg:          cfg,
		deltasSvc:    deltaSvc,
		bookTicksSvc: bookTicksSvc,
		snapshotsSvc: snapshotsSvc,
	}
}

func initConnections(cfg *conf.AppConfig) (*zk.Conn, *b2.Bucket, *gocql.Session) {
	zkConn, _, err := zk.Connect(cfg.ZkCfg.Servers, time.Second*time.Duration(cfg.ZkCfg.SessionTimeoutS))
	if err != nil {
		panic(err)
	}
	b2Client, err := b2.NewClient(context.TODO(), cfg.B2Cfg.Account, cfg.B2Cfg.Key)
	if err != nil {
		panic(err)
	}
	b2Bucket, err := b2Client.Bucket(context.TODO(), cfg.B2Cfg.Bucket)
	if err != nil {
		panic(err)
	}
	csCluster := gocql.NewCluster(cfg.SocratesCfg.Hosts...)
	csCluster.Keyspace = cfg.SocratesCfg.KeySpace
	session, err := csCluster.CreateSession()
	if err != nil {
		panic(err)
	}
	return zkConn, b2Bucket, session
}

func (s *App) Start() {
	baseContext := context.Background()
	go s.deltasSvc.Start(baseContext)
	go s.bookTicksSvc.Start(baseContext)
	go s.snapshotsSvc.Start(baseContext)
	time.Sleep(3 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		s.deltasSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.bookTicksSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.snapshotsSvc.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
	s.logger.Info("End of graceful shutdown")
}

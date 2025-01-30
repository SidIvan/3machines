package app

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	b2pqt "DeltaReceiver/internal/sizif/b2"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/internal/sizif/lock"
	"DeltaReceiver/internal/sizif/svc"
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
	logger   *zap.Logger
	cfg      *conf.AppConfig
	deltaSvc *svc.SizifSvc[model.Delta]
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
	return &App{
		logger:   logger,
		cfg:      cfg,
		deltaSvc: deltaSvc,
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
	go s.deltaSvc.Start(baseContext)
	time.Sleep(3 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		s.deltaSvc.Shutdown(ctx)
		wg.Done()
	}()
	s.logger.Info("End of graceful shutdown")
}

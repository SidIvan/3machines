package app

import (
	"DeltaReceiver/internal/common/web"
	"DeltaReceiver/internal/sizif/conf"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Backblaze/blazer/b2"
	"github.com/go-zookeeper/zk"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type App struct {
	logger         *zap.Logger
	cfg            *conf.AppConfig
	binanceSpotCtx *BinanceMarketCtx
	binanceUSDCtx  *BinanceMarketCtx
	binanceCoinCtx *BinanceMarketCtx
}

func NewApp(cfg *conf.AppConfig) *App {
	log.InitServiceName("sizif")
	logger := log.GetLogger("App")
	rawCfg, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(rawCfg))
	dwarfClient := web.NewDwarfHttpClient(cfg.DwarfURIConfig)
	zkConn, b2Bucket, csSession := initConnections(cfg)

	binanceSpotCtx := NewBinanceMarketCtx(bmodel.Spot, cfg.BinanceSpotCfg, cfg.SocratesCfg.BinanceSpotCfg, zkConn, b2Bucket, csSession, dwarfClient)
	binanceUSDCtx := NewBinanceMarketCtx(bmodel.FuturesUSD, cfg.BinanceUSDCfg, cfg.SocratesCfg.BinanceUSDCfg, zkConn, b2Bucket, csSession, dwarfClient)
	binanceCoinCtx := NewBinanceMarketCtx(bmodel.FuturesCoin, cfg.BinanceCoinCfg, cfg.SocratesCfg.BinanceCoinCfg, zkConn, b2Bucket, csSession, dwarfClient)

	return &App{
		logger:         logger,
		cfg:            cfg,
		binanceSpotCtx: binanceSpotCtx,
		binanceUSDCtx:  binanceUSDCtx,
		binanceCoinCtx: binanceCoinCtx,
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
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	go s.binanceSpotCtx.Start(baseContext)
	go s.binanceUSDCtx.Start(baseContext)
	go s.binanceCoinCtx.Start(baseContext)
	time.Sleep(3 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		s.binanceSpotCtx.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.binanceUSDCtx.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.binanceCoinCtx.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
	s.logger.Info("End of graceful shutdown")
}

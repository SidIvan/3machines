package app

import (
	cconf "DeltaReceiver/internal/common/conf"
	"DeltaReceiver/internal/nestor/conf"
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
	logger         *zap.Logger
	binanceSpotCtx *BinanceMarketCtx
	binanceUSDCtx  *BinanceMarketCtx
	binanceCoinCtx *BinanceMarketCtx
	cfg            *conf.AppConfig
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
	csCfg := cfg.CsCfg
	csSession := initCs(csCfg)
	binanceReconnectPeriod := time.Minute * time.Duration(cfg.ReconnectPeriodM)

	// binanceSpotCtx := NewBinanceMarketCtx(cfg.BinanceSpotCfg, csCfg.BinanceSpotCfg, csSession, binanceReconnectPeriod)
	binanceUSDCtx := NewBinanceMarketCtx(cfg.BinanceUSDCfg, csCfg.BinanceUSDCfg, csSession, binanceReconnectPeriod)
	binanceCoinCtx := NewBinanceMarketCtx(cfg.BinanceCoinCfg, csCfg.BinanceCoinCfg, csSession, binanceReconnectPeriod)
	return &App{
		logger:         logger,
		// binanceSpotCtx: binanceSpotCtx,
		binanceUSDCtx:  binanceUSDCtx,
		binanceCoinCtx: binanceCoinCtx,
		cfg:            cfg,
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
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(2 * time.Second)
	s.logger.Info("App started")
	// go s.binanceSpotCtx.Start(baseContext)
	// go s.binanceUSDCtx.Start(baseContext)
	go s.binanceCoinCtx.Start(baseContext)
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		// s.binanceSpotCtx.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		// s.binanceUSDCtx.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.binanceCoinCtx.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
	time.Sleep(30 * time.Second)
	s.logger.Info("End of graceful shutdown")
}

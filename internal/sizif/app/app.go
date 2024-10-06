package app

import (
	"DeltaReceiver/internal/common/repo"
	"DeltaReceiver/internal/common/repo/parquet"
	csvc "DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/internal/sizif/svc"
	"DeltaReceiver/pkg/clickhouse"
	"DeltaReceiver/pkg/log"
	"context"

	"go.uber.org/zap"
)

type App struct {
	logger         *zap.Logger
	cfg            *conf.AppConfig
	chPoolHolder   *clickhouse.ChPoolHolder
	parquetStorage svc.ParquetStorage
	deltaStorage   csvc.DeltaStorage
	sizifSvc       *svc.SizifSvc
}

func NewApp(cfg *conf.AppConfig) *App {
	log.InitServiceName("sizif")
	logger := log.GetLogger("App")
	chPoolHolder := clickhouse.NewChPoolHolder(cfg.GlobalRepoConfig.ChPoolCfg)
	deltaStorage := repo.NewChDeltaStorage(chPoolHolder, cfg.GlobalRepoConfig.DatabaseName, cfg.GlobalRepoConfig.DeltaTable)
	parquetStorage := parquet.NewS3ParquetStorage(cfg.ParquetStorageCfg.BasePath)
	sizifSvc := svc.NewSizifSvc(cfg, deltaStorage, parquetStorage)
	return &App{
		logger:         logger,
		cfg:            cfg,
		chPoolHolder:   chPoolHolder,
		parquetStorage: parquetStorage,
		deltaStorage:   deltaStorage,
		sizifSvc:       sizifSvc,
	}
}

func (s *App) Start() {
	baseContext := context.Background()
	if err := s.chPoolHolder.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
		return
	}
	s.logger.Info("App started")
	go s.sizifSvc.Start(baseContext)
}

func (s *App) Stop(ctx context.Context) {
	s.logger.Info("Begin of graceful shutdown")
	s.chPoolHolder.Disconnect()
	s.logger.Info("End of graceful shutdown")
}

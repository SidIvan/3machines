package app

import (
	"DeltaReceiver/internal/dwarf/api"
	"DeltaReceiver/internal/dwarf/cfg"
	"DeltaReceiver/internal/dwarf/metrics"
	"DeltaReceiver/internal/dwarf/repo"
	"DeltaReceiver/internal/dwarf/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type App struct {
	logger            *zap.Logger
	httpServer        *http.Server
	dwarfSvc          *svc.DwarfSvc
	deltaHolesStorage svc.HolesStorage
}

func NewApp(cfg *cfg.AppConfig) *App {
	log.InitServiceName("verbose")
	logger := log.GetLogger("App")
	deltaHolesStorage := repo.NewMongoDeltaHoleStorage(cfg.HolesStorageCfg)
	dwarfSvc := svc.NewDwarfSvc(deltaHolesStorage)
	metrics := metrics.NewApiMetrics()
	return &App{
		logger:   logger,
		dwarfSvc: dwarfSvc,
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.ListenPort),
			Handler: initApi(dwarfSvc, metrics, logger),
		},
		deltaHolesStorage: deltaHolesStorage,
	}
}

func (s *App) Start() {
	baseContext := context.Background()
	if err := s.deltaHolesStorage.Connect(baseContext); err != nil {
		s.logger.Error(err.Error())
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9001", nil)
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		err := s.httpServer.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(3 * time.Second)
	s.logger.Info("App started")
}

func (s *App) Stop(ctx context.Context) {
	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		s.logger.Info("Dwarf service shutdown gracefully")
	}
	s.logger.Error("Error while gracefull shutdown")
}

func initApi(dwarfSvc *svc.DwarfSvc, metrics svc.Metrics, logger *zap.Logger) http.Handler {
	nestorRouter := api.NewNestorRouter(dwarfSvc)
	r := mux.NewRouter()
	r.
		HandleFunc("/delta/hole", func(w http.ResponseWriter, r *http.Request) {
			serviceName := r.Header.Get(api.ServiceNameHeaderName)
			metrics.IncNumCallsCreateDeltaHole(serviceName)
			nestorRouter.SaveDeltasHoleHandler(w, r)
		}).
		Methods(http.MethodPost)
	r.
		HandleFunc("/delta/hole", func(w http.ResponseWriter, r *http.Request) {
			nestorRouter.GetDeltaHolesHandler(w, r)
		}).
		Methods(http.MethodGet)
	r.Use(log.CreateMiddleware(logger))
	return r
}

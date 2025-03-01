package api

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/dwarf/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.uber.org/zap"
)

type HolesRouter struct {
	logger   *zap.Logger
	dwarfSvc *svc.DwarfSvc
}

func NewHolesRouter(dwarfSvc *svc.DwarfSvc) *HolesRouter {
	return &HolesRouter{
		logger:   log.GetLogger("HolesRouter"),
		dwarfSvc: dwarfSvc,
	}
}

const ServiceNameHeaderName = "serviceName"

func (s *HolesRouter) SaveDeltasHoleHandler(w http.ResponseWriter, r *http.Request) {
	serviceName := r.Header.Get(ServiceNameHeaderName)
	s.logger.Debug("service name is " + serviceName)
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var deltaHole model.DeltaHole
	err = json.Unmarshal(body, &deltaHole)
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if ok := s.dwarfSvc.SaveDeltaHole(context.Background(), serviceName, deltaHole); ok {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *HolesRouter) GetDeltaHolesHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var reqBody svc.GetDeltaHolesRequest
	err = json.Unmarshal(body, &reqBody)
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	deltaHoles, err := s.dwarfSvc.GetDeltaHoles(context.Background(), &reqBody)
	s.logger.Debug(fmt.Sprintf("Got %d deltaHoles", len(deltaHoles)))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	respBody, err := json.Marshal(deltaHoles)
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	sentBytes, err := w.Write(respBody)
	if err != nil {
		s.logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if sentBytes != len(respBody) {
		s.logger.Warn("Not all response sent")
	}
	w.WriteHeader(http.StatusOK)
}

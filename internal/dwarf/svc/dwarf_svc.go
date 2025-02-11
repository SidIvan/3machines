package svc

import (
	cmodel "DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/dwarf/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type DwarfSvc struct {
	logger       *zap.Logger
	HolesStorage HolesStorage
}

func NewDwarfSvc(holesStorage HolesStorage) *DwarfSvc {
	return &DwarfSvc{
		logger:       log.GetLogger("DwarfSvc"),
		HolesStorage: holesStorage,
	}
}

type HolesStorage interface {
	Connect(context.Context) error
	SaveDeltaHole(context.Context, *model.DeltaHoleWithInfo) error
	GetDeltaHoles(context.Context, int64, int64) ([]model.DeltaHoleWithInfo, error)
}

type Metrics interface {
	IncNumCallsCreateDeltaHole(string)
}

func (s *DwarfSvc) SaveDeltaHole(ctx context.Context, serviceName string, deltaHole cmodel.DeltaHole) bool {
	deltasHole := model.NewDeltaHoleWithInfo(serviceName, &deltaHole)
	for i := 0; i < 3; i++ {
		if err := s.HolesStorage.SaveDeltaHole(ctx, deltasHole); err == nil {
			return true
		} else {
			s.logger.Error(err.Error())
		}
	}
	return false
}

type GetDeltaHolesRequest struct {
	FromTs RFC3339JSONTime `json:"timestamp_from"`
	ToTs   RFC3339JSONTime `json:"timestamp_to"`
}

type RFC3339JSONTime struct {
	ts time.Time
}

var (
	location, _ = time.LoadLocation("Europe/Moscow")
	layout      = "2006-01-02T15:04:05"
)

func (s *RFC3339JSONTime) UnmarshalJSON(b []byte) error {
	var err error
	stringTime := string(b)
	s.ts, err = time.ParseInLocation(layout, stringTime[1:len(stringTime)-1], location)
	if err != nil {
		return err
	}
	return nil
}

func (s *DwarfSvc) GetDeltaHoles(ctx context.Context, req *GetDeltaHolesRequest) ([]model.DeltaHoleWithInfo, error) {
	s.logger.Debug(fmt.Sprintf("Get delta holes request from %s to %s", req.FromTs.ts, req.ToTs.ts))
	deltaHoles, err := s.HolesStorage.GetDeltaHoles(ctx, req.FromTs.ts.UnixMilli(), req.ToTs.ts.UnixMilli())
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return deltaHoles, nil
}

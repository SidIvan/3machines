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
	IncNumCallsCreateDeltaHole()
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
	FromTs time.Time `json:"timestamp_from"`
	ToTs   time.Time `json:"timestamp_to"`
}

func (s *DwarfSvc) GetDeltaHoles(ctx context.Context, req *GetDeltaHolesRequest) ([]model.DeltaHoleWithInfo, error) {
	s.logger.Debug(fmt.Sprintf("Get delta holes request from %s to %s", req.FromTs, req.ToTs))
	deltaHoles, err := s.HolesStorage.GetDeltaHoles(ctx, req.FromTs.UnixMilli(), req.ToTs.UnixMilli())
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err
	}
	return deltaHoles, nil
}

package svc

import (
	cmodel "DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/dwarf/model"
	"DeltaReceiver/pkg/log"
	"context"

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

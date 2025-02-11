package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/nestor/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

const FixerBatchSize = 1000

type DeltaFixer struct {
	logger       *zap.Logger
	cfg          *conf.AppConfig
	deltaStorage DeltaStorage
	localRepo    LocalRepo
}

func NewDeltaFixer(cfg *conf.AppConfig, deltaStorage DeltaStorage, lRepo LocalRepo) *DeltaFixer {
	return &DeltaFixer{
		logger:       log.GetLogger("DeltaFixer"),
		cfg:          cfg,
		deltaStorage: deltaStorage,
		localRepo:    lRepo,
	}
}

const SleepTimeMin = 5

func (s *DeltaFixer) Fix() {
	ctx := context.Background()
	for {
		time.Sleep(1 * time.Second)
		deltasWithIds := s.getLocalSavedDeltas(ctx)
		if len(deltasWithIds) == 0 {
			s.logger.Debug("cannot get unsent deltas, sleep")
			time.Sleep(SleepTimeMin * time.Minute)
			continue
		}
		err := s.sendDeltas(ctx, deltasWithIds)
		if err != nil {
			s.logger.Error(fmt.Errorf("error while sending deltas, sleep: %w", err).Error())
			time.Sleep(SleepTimeMin * time.Minute)
			continue
		}
		err = s.deleteLocalDeltas(ctx, deltasWithIds)
		if err != nil {
			s.logger.Error(fmt.Errorf("error while deleting deltas, sleep: %w", err).Error())
			time.Sleep(SleepTimeMin * time.Minute)
			continue
		}
		s.logger.Info(fmt.Sprintf("successfully fixed %d deltas", len(deltasWithIds)))
	}
}

func (s *DeltaFixer) getLocalSavedDeltas(ctx context.Context) []model.DeltaWithId {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return s.localRepo.GetDeltas(ctxWithTimeout, BatchSize)
}

func (s *DeltaFixer) sendDeltas(ctx context.Context, deltasWithIds []model.DeltaWithId) error {
	var deltas []model.Delta
	for _, delta := range deltasWithIds {
		deltas = append(deltas, delta.GetDelta())
	}
	err := s.sendSmallBatchDeltas(ctx, deltas)
	if err == nil {
		return nil
	}
	return fmt.Errorf("batch not inserted %w", err)
}

func (s *DeltaFixer) sendSmallBatchDeltas(ctx context.Context, deltas []model.Delta) error {
	var err error
	for i := 0; i < 3; i++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = s.deltaStorage.SendDeltas(ctxWithTimeout, deltas)
		cancel()
		if err == nil {
			return nil
		}
		s.logger.Error(err.Error())
	}
	return err
}

func (s *DeltaFixer) deleteLocalDeltas(ctx context.Context, deltasWithIds []model.DeltaWithId) error {
	var ids []primitive.ObjectID
	for _, delta := range deltasWithIds {
		ids = append(ids, delta.MongoId)
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 15*time.Second)
	deletedNum, err := s.localRepo.DeleteDeltas(ctxWithTimeout, ids)
	cancel()
	if err != nil {
		s.logger.Error(err.Error())
	}
	if deletedNum != int64(len(deltasWithIds)) {
		s.logger.Warn("not all deltas deleted")
	}
	return err
}

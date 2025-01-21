package svc

import (
	"DeltaReceiver/internal/common/model"
	"context"
	"fmt"

	"go.uber.org/zap"
)

type SizifWorker[T any] struct {
	logger            *zap.Logger
	socratesStorage   SocratesStorage[T]
	parquetStorage    ParquetStorage[T]
	dataValidator     DataValidator[T]
	dataTransformator DataTransformator[T]
	taskQueue         <-chan model.ProcessingKey
	keyLocker         KeyLocker
}

func (s *SizifWorker[T]) Start(ctx context.Context) {
	for {
		s.lockAndProcessKey(ctx, <-s.taskQueue)
	}
}

func (s *SizifWorker[T]) lockAndProcessKey(ctx context.Context, key model.ProcessingKey) {
	s.logger.Debug(fmt.Sprintf("Start processing key %s", key.String()))
	for i := 0; i < 3; i++ {
		lockStatus, err := s.keyLocker.Lock(ctx, &key)
		if err != nil {
			s.logger.Error(err.Error())
			sleep(3)
			continue
		}
		if lockStatus == AlreadyLocked {
			s.logger.Debug(fmt.Sprintf("Key %s already processing or processed", key.String()))
			return
		}
	}
	for i := 0; i < 3; i++ {
		err := s.processKey(ctx, key)
		if err != nil {
			s.logger.Error(err.Error())
			sleep(3)
			continue
		}
		return
	}
}

func (s *SizifWorker[T]) processKey(ctx context.Context, key model.ProcessingKey) error {
	var data []T
	var err error
	for i := 0; i < 3; i++ {
		data, err = s.socratesStorage.Get(ctx, &key)
		if err == nil {
			break
		}
		s.logger.Error(err.Error())
	}
	isDataValid := s.dataValidator.Validate(data)
	if !isDataValid {
		s.logger.Warn(fmt.Sprintf("Invalid data for key %s", &key))
	}
	transformedData := s.dataTransformator.Transform(data)
	for i := 0; i < 3; i++ {
		err = s.parquetStorage.Save(ctx, transformedData, &key)
		if err == nil {
			for j := 0; j < 3; j++ {
				err = s.keyLocker.MarkProcessed(ctx, &key)
				if err == nil {
					return nil
				}
				s.logger.Error(err.Error())
			}
			return err
		}
		s.logger.Error(err.Error())
	}
	return err
}

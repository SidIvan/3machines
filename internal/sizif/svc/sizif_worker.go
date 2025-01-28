package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

type SizifWorker[T any] struct {
	logger            *zap.Logger
	shutdown          *atomic.Bool
	done              chan struct{}
	socratesStorage   SocratesStorage[T]
	parquetStorage    ParquetStorage[T]
	dataTransformator DataTransformator[T]
	taskQueue         <-chan model.ProcessingKey
	keyLocker         KeyLocker
}

func NewSizifWorker[T any](serviceType string, socratesStorage SocratesStorage[T], parquetStorage ParquetStorage[T], dataTransformator DataTransformator[T], taskQueue <-chan model.ProcessingKey, keyLocker KeyLocker) *SizifWorker[T] {
	var shutdown atomic.Bool
	shutdown.Store(false)
	done := make(chan struct{})
	return &SizifWorker[T]{
		logger:            log.GetLogger(fmt.Sprintf("SizifWorker[%s]", serviceType)),
		shutdown:          &shutdown,
		done:              done,
		socratesStorage:   socratesStorage,
		parquetStorage:    parquetStorage,
		dataTransformator: dataTransformator,
		taskQueue:         taskQueue,
		keyLocker:         keyLocker,
	}
}

func (s *SizifWorker[T]) Start(ctx context.Context) {
	for !s.shutdown.Load() {
		s.lockAndProcessKey(ctx, <-s.taskQueue)
	}
	s.done <- struct{}{}
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
	transformedData, isDataValid := s.dataTransformator.Transform(data, &key)
	if !isDataValid {
		s.logger.Warn(fmt.Sprintf("Invalid data for key %s", &key))
	}
	for i := 0; i < 3; i++ {
		err = s.parquetStorage.Save(ctx, transformedData, &key)
		if err == nil {
			for j := 0; j < 3; j++ {
				err = s.keyLocker.MarkProcessed(ctx, &key)
				if err == nil {
					for k := 0; k < 3; k++ {
						err = s.socratesStorage.Delete(ctx, &key)
						if err == nil {
							return nil
						}
						s.logger.Error(err.Error())
					}
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

func (s *SizifWorker[T]) Shutdown(ctx context.Context) {
	s.logger.Info("Start shutdown")
	s.shutdown.Store(true)
	<-s.done
	s.logger.Info("End shutdown")
}

package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
)

type SizifWorker[T model.WithTimestampMs] struct {
	logger            *zap.Logger
	shutdown          *atomic.Bool
	done              chan struct{}
	socratesStorage   SocratesStorage[T]
	parquetStorage    ParquetStorage[T]
	dataTransformator DataTransformator[T]
	taskQueue         <-chan model.ProcessingKey
	keyLocker         KeyLocker
}

func NewSizifWorker[T model.WithTimestampMs](serviceType string, socratesStorage SocratesStorage[T], parquetStorage ParquetStorage[T], dataTransformator DataTransformator[T], taskQueue <-chan model.ProcessingKey, keyLocker KeyLocker) *SizifWorker[T] {
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
	s.logger.Info("Worker started")
	for !s.shutdown.Load() {
		select {
		case key := <-s.taskQueue:
			s.lockAndProcessKey(ctx, key)
		default:
			sleep(5)
		}
		s.lockAndProcessKey(ctx, <-s.taskQueue)
	}
	s.logger.Info("Gracefully shutdown worker")
	s.done <- struct{}{}
}

func (s *SizifWorker[T]) lockAndProcessKey(ctx context.Context, key model.ProcessingKey) {
	s.logger.Debug(fmt.Sprintf("Start processing key %s", key.String()))
	lockStatus := s.lock(ctx, key)
	if lockStatus == AlreadyLocked {
		s.logger.Debug(fmt.Sprintf("Key %s already processing", key.String()))
		return
	} else if lockStatus == AlreadyProcessed {
		s.logger.Debug(fmt.Sprintf("Key %s already processed, delete it's data", key.String()))
		s.deleteKeyData(ctx, &key)
	} else {
		s.logger.Debug(fmt.Sprintf("Key %s not processed, start processing", key.String()))
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
}

func (s *SizifWorker[T]) lock(ctx context.Context, key model.ProcessingKey) LockOpStatus {
	for i := 0; i < 3; i++ {
		lockStatus, err := s.keyLocker.Lock(ctx, &key)
		if err != nil {
			s.logger.Error(err.Error())
			sleep(3)
			continue
		}
		return lockStatus
	}
	return AlreadyLocked
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
	if err != nil {
		errUnlock := s.keyLocker.Unlock(ctx, &key)
		if errUnlock == nil {
			s.logger.Info(fmt.Sprintf("key unlocked %s", &key))
		} else {
			s.logger.Warn(fmt.Sprintf("key not unlocked %s", &key))
		}
		return err
	}
	if len(data) == 0 {
		s.logger.Info(fmt.Sprintf("no data for key %s, delete it", &key))
		return s.socratesStorage.DeleteKey(ctx, &key)
	}
	transformedData, isDataValid := s.dataTransformator.Transform(data, &key)
	if !isDataValid {
		s.logger.Warn(fmt.Sprintf("Invalid data for key %s", &key))
	}
	for _, dataGroup := range transformedData {
		for i := 0; i < 3; i++ {
			err = s.parquetStorage.Save(ctx, dataGroup, dataGroup[0].GetTimestampMs(), &key)
			if err == nil {
				s.logger.Info(fmt.Sprintf("key %s saved to b2", &key))
				for j := 0; j < 3; j++ {
					err = s.keyLocker.MarkProcessed(ctx, &key)
					if err == nil {
						return s.deleteKeyData(ctx, &key)
					}
					s.logger.Error(err.Error())
				}
				return err
			}
			s.logger.Error(err.Error())
		}
	}
	return err
}

func (s *SizifWorker[T]) deleteKeyData(ctx context.Context, key *model.ProcessingKey) error {
	var err error
	for k := 0; k < 3; k++ {
		err = s.socratesStorage.Delete(ctx, key)
		if err == nil {
			for k := 0; k < 3; k++ {
				err = s.socratesStorage.DeleteKey(ctx, key)
				if err == nil {
					return nil
				}
				s.logger.Error(err.Error())
			}
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

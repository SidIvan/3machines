package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type SizifSvc[T model.WithTimestampMs] struct {
	logger          *zap.Logger
	socratesStorage SocratesStorage[T]
	workers         []*SizifWorker[T]
	taskQueue       chan<- model.ProcessingKey
}

func NewSizifSvc[T model.WithTimestampMs](serviceType string, socratesStorage SocratesStorage[T], parquetStorage ParquetStorage[T], dataTransformator DataTransformator[T], keyLocker KeyLocker, numWorkers int, metrics Metrics) *SizifSvc[T] {
	taskQueue := make(chan model.ProcessingKey, 1024)
	var workers []*SizifWorker[T]
	for i := 0; i < numWorkers; i++ {
		workers = append(workers, NewSizifWorker(serviceType, socratesStorage, parquetStorage, dataTransformator, taskQueue, keyLocker, metrics))
	}
	return &SizifSvc[T]{
		logger:          log.GetLogger(fmt.Sprintf("SizifSvc[%s]", serviceType)),
		socratesStorage: socratesStorage,
		workers:         workers,
		taskQueue:       taskQueue,
	}
}

func (s *SizifSvc[T]) Start(ctx context.Context) {
	for _, worker := range s.workers {
		go worker.Start(ctx)
	}
	s.logger.Info("Service started")
	for {
		newKeys := 0
		keys, err := s.socratesStorage.GetKeys(ctx)
		if err != nil {
			s.logger.Error(err.Error())
		} else {
			maxAllowedHourNo := cs.GetHourNo(time.Now().UnixMilli()) - 3
			for _, key := range keys {
				if key.HourNo <= maxAllowedHourNo {
					s.taskQueue <- key
					newKeys++
				}
			}
		}
		s.logger.Info(fmt.Sprintf("scheduled %d new keys", newKeys))
		sleep(60 * 5)
	}
}

func (s *SizifSvc[T]) Shutdown(ctx context.Context) {
	s.logger.Info("Start shutdown")
	var wg sync.WaitGroup
	wg.Add(len(s.workers))
	for _, worker := range s.workers {
		go func(worker *SizifWorker[T]) {
			worker.Shutdown(ctx)
			wg.Done()
		}(worker)
	}
	wg.Wait()
	s.logger.Info("End shutdown")
}

const ProcessingKeyLayout string = "2006-01-02T15:04:05"

func sleep(seconds int64) {
	time.Sleep(time.Duration(seconds) * time.Second)
}

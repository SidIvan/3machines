package svc

import (
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type DataFixer[T any] struct {
	logger      *zap.Logger
	mainStorage BatchedDataStorage[T]
	auxStorages []AuxBatchedDataStorage[T]
}

func NewDataFixer[T any](dataType string, mainStorage BatchedDataStorage[T], auxStorages []AuxBatchedDataStorage[T]) *DataFixer[T] {
	return &DataFixer[T]{
		logger:      log.GetLogger(fmt.Sprintf("DataFixer[%s]", dataType)),
		mainStorage: mainStorage,
		auxStorages: auxStorages,
	}
}

const SleepTimeMin = 5

func (s *DataFixer[T]) Fix() {
	ctx := context.Background()
	for {
		time.Sleep(1 * time.Second)
		for i, storage := range s.auxStorages {
			data, err, callback := storage.GetWithDeleteCallback(ctx)
			if err != nil {
				s.logger.Error(fmt.Errorf("error while getting data: %w", err).Error())
				break
			}
			if len(data) == 0 {
				s.logger.Info(fmt.Sprintf("no data in storage %d", i))
				continue
			}
			err = s.mainStorage.Save(ctx, data)
			if err != nil {
				s.logger.Error(fmt.Errorf("error while sending data, %w", err).Error())
				break
			}
			err = callback()
			if err != nil {
				s.logger.Error(fmt.Errorf("error while deleting data: %w", err).Error())
				break
			}
			s.logger.Info(fmt.Sprintf("successfully fixed %d data rows", len(data)))
		}
		time.Sleep(SleepTimeMin * time.Minute)
	}
}

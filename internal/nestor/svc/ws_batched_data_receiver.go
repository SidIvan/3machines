package svc

import (
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ErrNotSaved = errors.New("data not saved")
)

type WsDataProcessWorker[TRecv any, TResp any] struct {
	logger              *zap.Logger
	dataReceiver        DataReceiver[TRecv]
	dataTrasformator    DataTransformator[TRecv, TResp]
	batchSize           int
	dataStorages        []BatchedDataStorage[TResp]
	metrics             WsDataPipelineMetrics[TResp]
	saveDataWg          sync.WaitGroup
	shutdownCh          chan struct{}
	shutdownCompletedCh chan struct{}
}

func NewWsDataProcessWorker[TRecv, TResp any](
	dataType string,
	dataReceiver DataReceiver[TRecv],
	dataTrasformator DataTransformator[TRecv, TResp],
	batchSize int,
	dataStorages []BatchedDataStorage[TResp],
	metrics WsDataPipelineMetrics[TResp],
) *WsDataProcessWorker[TRecv, TResp] {
	return &WsDataProcessWorker[TRecv, TResp]{
		logger:              log.GetLogger(fmt.Sprintf("WsDataProcessWorker[%s]", dataType)),
		dataReceiver:        dataReceiver,
		dataTrasformator:    dataTrasformator,
		dataStorages:        dataStorages,
		batchSize:           batchSize,
		metrics:             metrics,
		saveDataWg:          sync.WaitGroup{},
		shutdownCh:          make(chan struct{}),
		shutdownCompletedCh: make(chan struct{}),
	}
}

func (s *WsDataProcessWorker[TRecv, TResp]) Start(ctx context.Context) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.dataReceiver.ConnectWs(ctxWithTimeout); err != nil {
		return fmt.Errorf("%w", err)
	}
	go s.Run(ctx)
	return nil
}

func (s *WsDataProcessWorker[TRecv, TResp]) Run(ctx context.Context) {
	for {
		select {
		case <-s.shutdownCh:
			s.saveDataWg.Wait()
			return
		default:
			s.RecvAndSaveBatch(ctx)
		}
	}
}

func (s *WsDataProcessWorker[TRecv, TResp]) RecvAndSaveBatch(ctx context.Context) {
	batch, err := s.Recv(ctx)
	if err != nil {
		s.metrics.IncRecvErr()
		s.logger.Error(err.Error())
		return
	}
	s.metrics.ProcessDataMetrics(batch, Receive)
	s.saveDataWg.Add(1)
	s.metrics.IncStartedSaveGoroutines()
	go func(ctx context.Context, batch []TResp) {
		defer s.saveDataWg.Done()
		defer s.metrics.IncEndedSaveGoroutines()
		err := s.Save(ctx, batch)
		if err != nil {
			s.logger.Error(err.Error())
		}
	}(ctx, batch)
}

func (s *WsDataProcessWorker[TRecv, TResp]) Recv(ctx context.Context) ([]TResp, error) {
	batch := make([]TResp, 0, s.batchSize)
	for len(batch) < s.batchSize {
		msg, err := s.dataReceiver.Recv(ctx)
		if err != nil {
			return nil, fmt.Errorf("data receiving error %w", err)
		}
		transformedData, err := s.dataTrasformator.Transform(msg)
		if transformedData == nil {
			s.logger.Warn("nil data batch")
		}
		if err != nil {
			return nil, fmt.Errorf("data transformation error %w", err)
		}
		batch = append(batch, transformedData...)
	}
	return batch, nil
}

func (s *WsDataProcessWorker[TRecv, TResp]) Save(ctx context.Context, batch []TResp) error {
	for i, storage := range s.dataStorages {
		for j := 0; j < 3; j++ {
			err := storage.Save(ctx, batch)
			if err == nil {
				if i > 0 {
					s.logger.Warn(fmt.Sprintf("data saved to additional storage with no = %d", i))
				}
				return nil
			} else {
				s.logger.Error(err.Error())
			}
		}
	}
	return ErrNotSaved
}

func (s *WsDataProcessWorker[TRecv, TResp]) Shutdown(ctx context.Context) {
	go func(ctx context.Context) {
		s.shutdownCh <- struct{}{}
		s.dataReceiver.Shutdown(ctx)
		s.shutdownCompletedCh <- struct{}{}
	}(ctx)
	<-s.shutdownCompletedCh
	s.logger.Debug("successfully shutdown")
}

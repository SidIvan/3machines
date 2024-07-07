package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"
)

type SizifSvc struct {
	logger         *zap.Logger
	deltaStorage   svc.DeltaStorage
	parquetStorage ParquetStorage
	mut            *sync.Mutex
	processing     map[int]map[string]struct{}
	cfg            *conf.AppConfig
}

type ProcessingKey struct {
	Date   string
	Symbol string
	Type   string
}

func NewSizifSvc(config *conf.AppConfig, deltaStorage svc.DeltaStorage, parquetStorage ParquetStorage) *SizifSvc {
	var mut sync.Mutex
	return &SizifSvc{
		logger:         log.GetLogger("SizifSvc"),
		deltaStorage:   deltaStorage,
		parquetStorage: parquetStorage,
		mut:            &mut,
		processing:     make(map[int]map[string]struct{}),
		cfg:            config,
	}
}

func (s *SizifSvc) Start(ctx context.Context) {
	for i := 0; i < s.cfg.NumWorkerThreads; i++ {
		go s.startSingleProcess(ctx)
	}
}

const (
	SecondsInHour = 60 * 60
	SecondsInDay  = 24 * SecondsInHour
)

var (
	reschedulePeriodWholeProcessS = []int64{1, 3, 10, 20, 60, 120, 300, 600}
	reschedulePeriodGetDeltasS    = []int64{1, 3, 10, 20}
	rescheduleSaveParquetS        = []int64{1, 3, 10, 20}
)

func (s *SizifSvc) startSingleProcess(ctx context.Context) {
	for {
		timestamp, err := s.deltaStorage.GetEarliestTs(ctx)
		if errors.Is(err, svc.EmptyStorage) {
			return
		}
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		dayNumber := timestamp.Second() / SecondsInDay
		currentDayNumber := time.Now().Second() / SecondsInDay
		if currentDayNumber-dayNumber < 2 {
			time.Sleep(1 * time.Hour)
			continue
		}
		s.logger.Debug(fmt.Sprintf("earliest ts is %d processing day %d", timestamp, dayNumber))
		fromS := int64(dayNumber) * SecondsInDay
		toS := int64(dayNumber) * SecondsInDay
		for _, reschedulePeriodS := range reschedulePeriodWholeProcessS {
			s.mut.Lock()
			if s.processing[dayNumber] == nil {
				s.processing[dayNumber] = make(map[string]struct{})
			}
			symbol, err := s.deltaStorage.GetSymbol(ctx, fromS, toS, s.processing[dayNumber])
			if err != nil {
				s.logger.Error(err.Error())
				s.mut.Unlock()
				time.Sleep(time.Duration(reschedulePeriodS) * time.Second)
				continue
			}
			s.processing[dayNumber][symbol] = struct{}{}
			s.mut.Unlock()
			date := time.Unix(fromS, 0).Format("2006-01-02")
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				if err := s.ProcessKey(ctx, &ProcessingKey{
					Date:   date,
					Symbol: symbol,
					Type:   "bid",
				}, fromS, toS); err != nil {
					s.logger.Error(err.Error())
				}
				wg.Done()
			}()
			go func() {
				if err := s.ProcessKey(ctx, &ProcessingKey{
					Date:   date,
					Symbol: symbol,
					Type:   "ask",
				}, fromS, toS); err != nil {
					s.logger.Error(err.Error())
				}
				wg.Done()
			}()
			wg.Wait()
			s.mut.Lock()
			delete(s.processing[dayNumber], symbol)
			s.mut.Unlock()
		}
	}
}

func (s *SizifSvc) ProcessKey(ctx context.Context, key *ProcessingKey, fromS, toS int64) error {
	if s.parquetStorage.IsParquetExists(key) {
		s.logger.Debug(fmt.Sprintf("%s already processed", key.String()))
		return nil
	}
	s.logger.Info(fmt.Sprintf("start processing %s", key.String()))
	var deltas []model.Delta
	var err error
	for ; fromS != toS; fromS += SecondsInHour {
		var gotDeltas []model.Delta
		for _, rescheduleS := range reschedulePeriodGetDeltasS {
			gotDeltas, err = s.deltaStorage.GetDeltas(ctx, key.Symbol, fromS, fromS+SecondsInHour)
			if err != nil {
				s.logger.Error(err.Error())
				time.Sleep(time.Duration(rescheduleS) * time.Second)
				continue
			}
			break
		}
		s.logger.Debug(fmt.Sprintf("god %d deltas for %s", len(gotDeltas), key.String()))
		if err != nil {
			s.deltaStorage.Reconnect(ctx)
			return err
		}
		deltas = append(deltas, gotDeltas...)
	}
	s.logger.Info(fmt.Sprintf("downloaded %d deltas for %s", len(deltas), key.String()))
	if s.parquetStorage.IsParquetExists(key) {
		return nil
	}
	for _, rescheduleS := range rescheduleSaveParquetS {
		err = s.parquetStorage.SaveDeltas(deltas, key)
		if err != nil {
			s.logger.Error(err.Error())
			time.Sleep(time.Duration(rescheduleS) * time.Second)
			continue
		}
		s.logger.Info(fmt.Sprintf("%s saved to parquet successfully", key.String()))
		break
	}
	if s.cfg.IsDeleteProcessedDeltas {
		err = s.deltaStorage.DeleteDeltas(ctx, key.Symbol, fromS, toS, key.Type)
		if err != nil {
			s.logger.Error(err.Error())
			return err
		}
		s.logger.Info(fmt.Sprintf("%s deleted from temporary storage", key.String()))
	}
	return nil
}

func (s *ProcessingKey) String() string {
	return fmt.Sprintf("[%s, %s, %s]", s.Symbol, s.Type, s.Date)
}

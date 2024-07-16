package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/internal/sizif/cache"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"sort"
	"sync"
	"time"
)

type SizifSvc struct {
	logger                 *zap.Logger
	holeLogger             *zap.Logger
	deltaStorage           svc.DeltaStorage
	parquetStorage         ParquetStorage
	mut                    *sync.Mutex
	processing             map[ProcessingKey]struct{}
	unprocessedDeltasCache *cache.UnprocessedDeltasCache
	cfg                    *conf.AppConfig
}

type ProcessingKey struct {
	DateTimeStart string
	DateTimeEnd   string
	Symbol        string
}

func (s *ProcessingKey) GetStartTime() time.Time {
	startTime, _ := time.Parse(ProcessingKeyLayout, s.DateTimeStart)
	return startTime
}

func (s *ProcessingKey) GetEndTime() time.Time {
	endTime, _ := time.Parse(ProcessingKeyLayout, s.DateTimeEnd)
	return endTime
}

const ProcessingKeyLayout string = "2006-01-02T15:04:05"

func NewSizifSvc(config *conf.AppConfig, deltaStorage svc.DeltaStorage, parquetStorage ParquetStorage) *SizifSvc {
	var mut sync.Mutex
	return &SizifSvc{
		logger:                 log.GetLogger("SizifSvc"),
		holeLogger:             log.GetCustomLogger("Hole", "holes"),
		deltaStorage:           deltaStorage,
		parquetStorage:         parquetStorage,
		mut:                    &mut,
		processing:             make(map[ProcessingKey]struct{}),
		unprocessedDeltasCache: cache.NewUnprocessedDeltasCache(deltaStorage),
		cfg:                    config,
	}
}

func (s *SizifSvc) Start(ctx context.Context) {
	firstDayFromConf, err := time.Parse(ProcessingKeyLayout, s.cfg.ProcessDeltasFrom)
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	var wg sync.WaitGroup
	wg.Add(s.cfg.NumWorkerThreads)
	for i := 0; i < s.cfg.NumWorkerThreads; i++ {
		go func() {
			s.logger.Info("worker thread started")
			s.startSingleProcess(ctx, firstDayFromConf)
			wg.Done()
		}()
	}
	wg.Wait()
}

const (
	SecondsInMinute = int64(60)
	MinutesInHour   = int64(60)
	HoursInDay      = int64(24)
	SecondsInHour   = SecondsInMinute * MinutesInHour
	SecondsInDay    = SecondsInHour * HoursInDay
)

var (
	reschedulePeriodWholeProcessS = []int64{1, 3, 10, 20, 60, 120, 300, 600}
	reschedulePeriodGetDeltasS    = []int64{1, 3, 10, 20, 60}
	rescheduleSaveParquetS        = []int64{1, 3, 10, 20, 60}
	rescheduleDeleteDeltasS       = []int64{1, 3, 10, 20, 60}
)

func sleep(seconds int64) {
	time.Sleep(time.Duration(seconds) * time.Second)
}

func (s *SizifSvc) getDeltas(ctx context.Context, pKey *ProcessingKey) ([]model.Delta, error) {
	var deltas []model.Delta
	var err error
	fromTime := pKey.GetStartTime()
	toTime := pKey.GetEndTime()
	for _, reschedulePeriodS := range reschedulePeriodGetDeltasS {
		deltas, err = s.deltaStorage.GetDeltas(ctx, pKey.Symbol, "", fromTime, toTime)
		if err == nil {
			return deltas, nil
		}
		s.logger.Error(err.Error())
		sleep(reschedulePeriodS)
	}
	deltas, err = s.deltaStorage.GetDeltas(ctx, pKey.Symbol, "", fromTime, toTime)
	if err != nil {
		s.logger.Error(err.Error())
	}
	return deltas, err
}

func (s *SizifSvc) saveParquet(ctx context.Context, deltas []model.Delta, key *ProcessingKey) error {
	if s.parquetStorage.IsParquetExists(key) {
		return ParquetAlreadyExists
	}
	for _, reschedulePeriodS := range rescheduleSaveParquetS {
		err := s.parquetStorage.SaveDeltas(deltas, key)
		if err == nil {
			return nil
		}
		s.logger.Error(fmt.Errorf("saving parquet error %w", err).Error())
		sleep(reschedulePeriodS)
	}
	err := s.parquetStorage.SaveDeltas(deltas, key)
	if err != nil {
		s.logger.Error(fmt.Errorf("saving parquet error %w", err).Error())
	}
	return err
}

var deltaTypes = []string{"bid", "ask"}

func (s *SizifSvc) startSingleProcess(ctx context.Context, since time.Time) {
	for ; ; time.Sleep(5 * time.Minute) {
		symbToTsSegment, err := s.unprocessedDeltasCache.GetUnprocessedDeltas(ctx, since)
		if err != nil {
			s.logger.Error(err.Error())
			continue
		}
		for symbol, timePair := range symbToTsSegment {
			firstDayNo := timePair.Earliest.Unix() / SecondsInDay
			lastDayNo := timePair.Latest.Unix() / SecondsInDay
			for curDayNo := firstDayNo; curDayNo < lastDayNo; curDayNo++ {
				for hourNo := int64(0); hourNo < 24; hourNo++ {
					s.logger.Info(fmt.Sprintf("%d %d %d %d", curDayNo, lastDayNo, curDayNo*SecondsInDay+hourNo*SecondsInHour, curDayNo*SecondsInDay+(hourNo+1)*SecondsInHour))
					curProcDate := time.Unix(curDayNo*SecondsInDay+hourNo*SecondsInHour, 0).Format(ProcessingKeyLayout)
					curEndProcDate := time.Unix(curDayNo*SecondsInDay+(hourNo+1)*SecondsInHour, 0).Format(ProcessingKeyLayout)
					s.logger.Info(curProcDate)
					s.logger.Info(curEndProcDate)
					curProcKey := ProcessingKey{
						DateTimeStart: curProcDate,
						DateTimeEnd:   curEndProcDate,
						Symbol:        symbol,
					}
					s.mut.Lock()
					if _, ok := s.processing[curProcKey]; ok {
						s.mut.Unlock()
						continue
					}
					s.processing[curProcKey] = struct{}{}
					s.mut.Unlock()
					for i := 0; i < 3; i++ {
						if err := s.ProcessKey(ctx, &curProcKey); err != nil {
							s.logger.Error(err.Error())
						} else {
							break
						}
					}
					s.mut.Lock()
					delete(s.processing, curProcKey)
					s.mut.Unlock()

				}
			}
		}
		s.logger.Info("end of processing, sleep 5 min")
	}
}

func (s *SizifSvc) ProcessKey(ctx context.Context, key *ProcessingKey) error {
	if s.parquetStorage.IsParquetExists(key) {
		s.logger.Debug(fmt.Sprintf("%s already processed", key.String()))
		return nil
	}
	s.logger.Info(fmt.Sprintf("start processing %s", key.String()))
	for _, reschedulePeriodS := range reschedulePeriodWholeProcessS {
		if err := s.attemptToProcessKey(ctx, key); err != nil {
			sleep(reschedulePeriodS)
			continue
		}
		return nil
	}
	return s.attemptToProcessKey(ctx, key)
}

func (s *SizifSvc) attemptToProcessKey(ctx context.Context, key *ProcessingKey) error {
	deltas, err := s.getDeltas(ctx, key)
	if err != nil {
		return err
	}
	s.logger.Info(fmt.Sprintf("downloaded %d deltas for %s", len(deltas), key.String()))
	if s.parquetStorage.IsParquetExists(key) {
		s.logger.Warn(fmt.Sprintf("downloaded %d deltas for %s", len(deltas), key.String()))
		return nil
	}
	deltas = s.validateAndRemoveDuplicates(deltas, key)
	err = s.saveParquet(ctx, deltas, key)
	if err != nil {
		return err
	}
	if s.cfg.IsDeleteProcessedDeltas {
		return s.deleteDeltas(ctx, key)
	}
	return nil
}

func (s *SizifSvc) validateAndRemoveDuplicates(deltas []model.Delta, pKey *ProcessingKey) []model.Delta {
	if len(deltas) == 0 {
		return nil
	}
	fromTime := pKey.GetStartTime()
	toTime := pKey.GetEndTime()
	var validatedDeltas []model.Delta
	for _, delta := range deltas {
		deltaTs := time.UnixMilli(delta.Timestamp)
		if delta.Symbol != pKey.Symbol ||
			deltaTs.Before(fromTime) ||
			deltaTs.After(toTime) {
			s.logger.Error(fmt.Sprintf("deltas are not valid to key, delta = [%s], key = [%s]", delta, pKey))
			return nil
		}
	}
	sort.Slice(deltas, func(i, j int) bool {
		return deltas[i].UpdateId < deltas[j].UpdateId ||
			(deltas[i].UpdateId == deltas[j].UpdateId && deltas[i].FirstUpdateId < deltas[j].FirstUpdateId)
	})
	validatedDeltas = append(validatedDeltas, deltas[0])
	for i := 1; i < len(deltas); i++ {
		delta := deltas[i]
		lastValidatedDelta := validatedDeltas[len(validatedDeltas)-1]
		if delta.UpdateId == lastValidatedDelta.UpdateId &&
			delta.T == lastValidatedDelta.T &&
			delta.Price == lastValidatedDelta.Price &&
			delta.Timestamp == lastValidatedDelta.Timestamp &&
			delta.Count == lastValidatedDelta.Count {
			continue
		}
		expectedFirstUpdateId := lastValidatedDelta.UpdateId + 1
		if delta.FirstUpdateId > expectedFirstUpdateId {
			logHole(s.holeLogger, delta.Symbol, delta.GetStringDeltaType(), expectedFirstUpdateId, delta.FirstUpdateId-1, lastValidatedDelta.Timestamp)
		}
		validatedDeltas = append(validatedDeltas, delta)
	}
	return validatedDeltas
}

type HoleMsg struct {
	Symbol     string `json:"symbol"`
	FirstUpdId int64  `json:"first_update_id"`
	LastUpdId  int64  `json:"last_update_id"`
	TsMs       int64  `json:"timestamp_ms"`
}

func logHole(logger *zap.Logger, symbol, deltaType string, sinceUpdId, toUpdId, tsMs int64) {
	holeMsg := HoleMsg{
		Symbol:     symbol,
		FirstUpdId: sinceUpdId,
		LastUpdId:  toUpdId,
		TsMs:       tsMs,
	}
	byteMsg, err := json.Marshal(holeMsg)
	if err != nil {
		panic(err)
	}
	logger.Info(string(byteMsg))
}

func (s *SizifSvc) deleteDeltas(ctx context.Context, pKey *ProcessingKey) error {
	startTime := pKey.GetStartTime()
	endTime := pKey.GetEndTime()
	for _, reschedulePeriod := range rescheduleDeleteDeltasS {
		if err := s.deltaStorage.DeleteDeltas(ctx, pKey.Symbol, startTime, endTime); err != nil {
			s.logger.Error(fmt.Errorf("deleting deltas error %w", err).Error())
			sleep(reschedulePeriod)
			continue
		}
		return nil
	}
	err := s.deltaStorage.DeleteDeltas(ctx, pKey.Symbol, startTime, endTime)
	if err != nil {
		s.logger.Error(fmt.Errorf("deleting deltas error %w", err).Error())
	}
	return err
}

func (s *ProcessingKey) String() string {
	return fmt.Sprintf("[%s, %s, %s]", s.Symbol, s.DateTimeStart, s.DateTimeEnd)
}

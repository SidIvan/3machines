package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/sizif/conf"
	"context"
	"encoding/json"
	"sort"
	"time"

	"go.uber.org/zap"
)

type SizifSvc[T any] struct {
	logger          *zap.Logger
	socratesStorage SocratesStorage[T]
	workers         []SizifWorker[T]
	taskQueue       chan<- model.ProcessingKey
	cfg             *conf.AppConfig
}

func (s *SizifSvc[T]) Start(ctx context.Context) {
	for _, worker := range s.workers {
		worker.Start(ctx)
	}
	for {
		keys, err := s.socratesStorage.GetKeys(ctx)
		if err != nil {
			s.logger.Error(err.Error())
		} else {
			for _, key := range keys {
				s.taskQueue <- key
			}
		}
		sleep(10 * 60)
	}
}

const ProcessingKeyLayout string = "2006-01-02T15:04:05"

func sleep(seconds int64) {
	time.Sleep(time.Duration(seconds) * time.Second)
}

var location, _ = time.LoadLocation("Europe/Moscow")

func validateAndRemoveDuplicates(deltas []model.Delta, pKey *model.ProcessingKey) []model.Delta {
	if len(deltas) == 0 {
		return nil
	}
	// s.logger.Info(fmt.Sprintf("start validate %s", *pKey))
	fromTime := pKey.GetStartTime()
	toTime := pKey.GetEndTime()
	var validatedDeltas []model.Delta
	for _, delta := range deltas {
		deltaTs := time.UnixMilli(delta.Timestamp).In(location)
		if delta.Symbol != pKey.Symbol ||
			deltaTs.Before(fromTime) ||
			deltaTs.After(toTime) {
			// s.logger.Error(fmt.Sprintf("deltas are not valid to key, delta = [%s], key = [%s]", delta.String(), *pKey))
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
			// logHole(s.holeLogger, delta.Symbol, delta.GetStringDeltaType(), expectedFirstUpdateId, delta.FirstUpdateId-1, lastValidatedDelta.Timestamp)
		}
		validatedDeltas = append(validatedDeltas, delta)
	}
	// s.logger.Info(fmt.Sprintf("end validate %s", *pKey))
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

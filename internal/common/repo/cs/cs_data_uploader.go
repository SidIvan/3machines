package cs

import (
	"DeltaReceiver/internal/common/model"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsDataUploader[T model.BinanceDataRow] struct {
	logger                     *zap.Logger
	session                    *gocql.Session
	metrics                    CsStorageMetrics
	insertQueryBuilder         InsertQueryBuilder[T]
	insertedKeys               map[model.ProcessingKey]struct{}
	insertedKeysMut            *sync.Mutex
	lastInsertedKeysClearingTs time.Time
	keyInsertQueryBuilder      *KeyInsertQueryBuilder
}

const microBatchSize int = 100

func NewCsDataUploader[T model.BinanceDataRow](logger *zap.Logger, session *gocql.Session, metrics CsStorageMetrics, keysTableName string, insertQueryBuilder InsertQueryBuilder[T]) *CsDataUploader[T] {
	var mut sync.Mutex
	return &CsDataUploader[T]{
		logger:                     logger,
		session:                    session,
		metrics:                    metrics,
		insertQueryBuilder:         insertQueryBuilder,
		insertedKeys:               make(map[model.ProcessingKey]struct{}),
		insertedKeysMut:            &mut,
		lastInsertedKeysClearingTs: time.UnixMilli(0),
		keyInsertQueryBuilder:      NewKeyInsertQueryBuilder(keysTableName),
	}
}

func (s CsDataUploader[T]) UploadData(ctx context.Context, data []T) error {
	keyToDeltas := SplitDataToBatches(data)
	var wg sync.WaitGroup
	var numKeysSuccessfullySentKeys atomic.Int32
	for key, deltas := range keyToDeltas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.sendPartition(ctx, key, deltas)
			if err != nil {
				s.logger.Error(err.Error())
			} else {
				numKeysSuccessfullySentKeys.Add(1)
			}
		}()
	}
	wg.Wait()
	if numKeysSuccessfullySentKeys.Load() == int32(len(keyToDeltas)) {
		var keys []model.ProcessingKey
		for key := range keyToDeltas {
			keys = append(keys, key)
		}
		err := s.sendNewKeys(ctx, keys)
		if err != nil {
			s.logger.Error(err.Error())
			return fmt.Errorf("keys not saved %w", err)
		}
		return nil
	}
	return errors.New("batch not saved")
}

func (s CsDataUploader[T]) sendPartition(ctx context.Context, key model.ProcessingKey, data []T) error {
	var wg sync.WaitGroup
	var errorFlag atomic.Bool
	errorFlag.Store(false)
	for i := 0; i < len(data) || errorFlag.Load(); i += microBatchSize {
		wg.Add(1)
		go func(batch []T) {
			defer wg.Done()
			errs := s.sendMicroBatch(ctx, key, batch)
			if len(errs) != 0 {
				s.logger.Error(fmt.Errorf("got %d errors when inserting micro batches, example: %w", len(errs), errs[0]).Error())
				errorFlag.Store(true)
			}
		}(data[i:min(len(data), i+microBatchSize)])
	}
	wg.Wait()
	if errorFlag.Load() {
		return errors.New("partition not saved")
	}
	return nil
}

func (s CsDataUploader[T]) sendMicroBatch(ctx context.Context, key model.ProcessingKey, data []T) []error {
	var errs []error
	for range 3 {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*3)
		err := s.upload(ctxWithTimeout, key, data)
		if err == nil {
			cancel()
			return nil
		}
		errs = append(errs, err)
		cancel()
	}
	return errs
}

func (s CsDataUploader[T]) upload(ctx context.Context, key model.ProcessingKey, data []T) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, row := range data {
		s.insertQueryBuilder.BuildQuery(batch, key, row)
	}
	startQueryMs := time.Now().UnixMilli()
	err := s.session.ExecuteBatch(batch)
	if err != nil {
		return err
	}
	s.metrics.UpdInsertQueryLatency(time.Now().UnixMilli() - startQueryMs)
	return nil
}

func (s CsDataUploader[T]) sendNewKeys(ctx context.Context, keys []model.ProcessingKey) error {
	now := time.Now()
	s.insertedKeysMut.Lock()
	defer s.insertedKeysMut.Unlock()
	if now.Sub(s.lastInsertedKeysClearingTs) < time.Hour*3 {
		curHour := GetHourNo(now.UnixMilli())
		clearedSentKeys := make(map[model.ProcessingKey]struct{})
		for key := range s.insertedKeys {
			if key.HourNo > curHour-3 {
				clearedSentKeys[key] = struct{}{}
			}
		}
		s.insertedKeys = clearedSentKeys
		s.lastInsertedKeysClearingTs = now
	}
	var keysToInsert []model.ProcessingKey
	for _, key := range keys {
		if _, ok := s.insertedKeys[key]; !ok {
			keysToInsert = append(keysToInsert, key)
		}
	}
	if len(keysToInsert) == 0 {
		return nil
	}
	var err error
	for range 3 {
		err = s.sendKeys(ctx, keysToInsert)
		if err == nil {
			for _, key := range keysToInsert {
				s.insertedKeys[key] = struct{}{}
			}
			return nil
		}
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsDataUploader[T]) sendKeys(ctx context.Context, keys []model.ProcessingKey) error {
	for i := 0; i < len(keys); i += microBatchSize {
		batch := keys[i:min(len(keys), i+microBatchSize)]
		err := s.sendKeysMicroBatch(ctx, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s CsDataUploader[T]) sendKeysMicroBatch(ctx context.Context, keys []model.ProcessingKey) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctxWithTimeout)
	batch.SetConsistency(gocql.LocalQuorum)
	for _, row := range keys {
		s.keyInsertQueryBuilder.BuildQuery(batch, row)
	}
	return s.session.ExecuteBatch(batch)
}

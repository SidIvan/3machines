package cs

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsDeltaStorage struct {
	logger          *zap.Logger
	session         *gocql.Session
	tableName       string
	insertStatement string
}

func NewCsDeltaStorage(session *gocql.Session, tableName string) *CsDeltaStorage {
	logger := log.GetLogger("CsDeltaStorage")
	deltaStorage := &CsDeltaStorage{
		logger:    logger,
		session:   session,
		tableName: tableName,
	}
	deltaStorage.initStatements()
	return deltaStorage
}

func (s *CsDeltaStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (symbol, hour, timestamp_ms, type, price, count, first_update_id, update_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", s.tableName)
}

func (s CsDeltaStorage) SendDeltas(ctx context.Context, deltas []model.Delta) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, delta := range deltas {
		batch.Query(s.insertStatement, delta.Symbol, getHourNo(delta.Timestamp), delta.Timestamp, delta.T, delta.Price, delta.Count, delta.FirstUpdateId, delta.UpdateId)
	}
	err := s.session.ExecuteBatch(batch)
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsDeltaStorage) GetDeltas(ctx context.Context, symbol, deltaType string, fromTime, toTime time.Time) ([]model.Delta, error) {
	return nil, nil
}

func (s CsDeltaStorage) DeleteDeltas(ctx context.Context, symbol string, fromTime, toTime time.Time) error {
	return nil
}

func (s CsDeltaStorage) GetTsSegment(ctx context.Context, since time.Time) (map[string]svc.TimePair, error) {
	return nil, nil
}

func (s CsDeltaStorage) Connect(ctx context.Context) error {
	return nil
}

func (s CsDeltaStorage) Reconnect(ctx context.Context) error {
	return nil
}

func (s CsDeltaStorage) Disconnect(ctx context.Context) {
	s.session.Close()
}

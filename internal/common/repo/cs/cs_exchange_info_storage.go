package cs

import (
	"DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type CsExchangeInfoStorage struct {
	logger                     *zap.Logger
	session                    *gocql.Session
	tableName                  string
	insertStatement            string
	selectDaysExInfoStatemenet string
	selectLastExInfoStatemenet string
}

func NewExchangeInfoStorage(session *gocql.Session, tableName string) *CsExchangeInfoStorage {
	logger := log.GetLogger("CsExchangeInfoStorage")
	exchangeInfoStorage := &CsExchangeInfoStorage{
		logger:    logger,
		session:   session,
		tableName: tableName,
	}
	exchangeInfoStorage.initStatements()
	return exchangeInfoStorage
}

func (s *CsExchangeInfoStorage) initStatements() {
	s.insertStatement = fmt.Sprintf("INSERT INTO %s (day, timestamp_ms, ex_info_hash, ex_info) VALUES (?, ?, ?, ?)", s.tableName)
	s.selectDaysExInfoStatemenet = fmt.Sprintf("SELECT day FROM %s", s.tableName)
	s.selectLastExInfoStatemenet = fmt.Sprintf("SELECT ex_info FROM %s WHERE day = ? ORDER BY timestamp_ms DESC LIMIT 1", s.tableName)
}

func (s CsExchangeInfoStorage) SendExchangeInfo(ctx context.Context, exInfo *model.ExchangeInfo) error {
	payload, err := json.Marshal(exInfo)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	curTsMs := exInfo.ServerTime
	query := s.session.Query(s.insertStatement, getDayNo(curTsMs), curTsMs, exInfo.ExInfoHash(), payload).WithContext(ctx)
	err = query.Exec()
	if err != nil {
		s.logger.Error(err.Error())
	}
	return err
}

func (s CsExchangeInfoStorage) GetLastExchangeInfo(context.Context) *model.ExchangeInfo {
	lastDay := s.GetLastDayExInfo(context.Background())
	if lastDay == -1 {
		return nil
	}
	respIt := s.session.Query(s.selectLastExInfoStatemenet, lastDay).Iter()
	defer respIt.Close()
	var rawVal string
	respIt.Scan(&rawVal)
	var exInfo model.ExchangeInfo
	err := json.Unmarshal([]byte(rawVal), &exInfo)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	return &exInfo
}

func (s CsExchangeInfoStorage) GetLastDayExInfo(ctx context.Context) int64 {
	respIt := s.session.Query(s.selectDaysExInfoStatemenet).Iter()
	defer respIt.Close()
	var maxDay int64 = -1
	var day int64
	for respIt.Scan(&day) {
		maxDay = max(day, maxDay)
	}
	return maxDay
}

func (s CsExchangeInfoStorage) Connect(ctx context.Context) error {
	return nil
}

func (s CsExchangeInfoStorage) Reconnect(ctx context.Context) error {
	return nil
}

func (s CsExchangeInfoStorage) Disconnect(ctx context.Context) {
	if !s.session.Closed() {
		s.session.Close()
	}
}

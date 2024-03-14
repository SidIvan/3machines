package repo

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
	"hash/fnv"
	"strconv"
	"sync"
	"time"
)

const (
	TimestampCol = "timestamp"
	SymbolCol    = "symbol"
)

type chClientHolder struct {
	client *ch.Client
	mut    sync.Mutex
}

type ClickhouseRepo struct {
	clientH *chClientHolder
	logger  *zap.Logger
	cfg     *conf.GlobalRepoConfig
}

func (s ClickhouseRepo) Reconnect(ctx context.Context) {
	s.clientH.mut.Lock()
	defer s.clientH.mut.Unlock()
	if client, err := ch.Dial(ctx, ch.Options{
		Address: s.cfg.URI.GetAddress(),
	}); err != nil {
		s.logger.Error(err.Error())
	} else {
		s.clientH.client = client
	}
}

func NewClickhouseRepo(cfg *conf.GlobalRepoConfig) *ClickhouseRepo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
	defer cancel()
	logger := log.GetLogger("ClickhouseRepo")
	client, err := ch.Dial(ctx, ch.Options{
		Address: cfg.URI.GetAddress(),
	})
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	return &ClickhouseRepo{
		logger: logger,
		clientH: &chClientHolder{
			client: client,
			mut:    sync.Mutex{},
		},
		cfg: cfg,
	}
}

func prepareDeltasInsertBlock(deltas []model.Delta) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var updateIdCol proto.ColInt64
	var firstUpdateIdCol proto.ColInt64
	var symbCol proto.ColEnum
	for _, delta := range deltas {
		timestampCol.Append(time.UnixMilli(delta.Timestamp))
		if delta.T {
			typeCol.Append("bid")
		} else {
			typeCol.Append("ask")
		}
		priceCol.Append(delta.Price)
		countCol.Append(delta.Count)
		updateIdCol.Append(delta.UpdateId)
		firstUpdateIdCol.Append(delta.FirstUpdateId)
		symbCol.Append(string(delta.Symbol))
	}
	return proto.Input{
		{Name: TimestampCol, Data: timestampCol},
		{Name: "delta_type", Data: &typeCol},
		{Name: "price", Data: &priceCol},
		{Name: "count", Data: &countCol},
		{Name: "update_id", Data: &updateIdCol},
		{Name: "first_update_id", Data: &firstUpdateIdCol},
		{Name: SymbolCol, Data: &symbCol},
	}
}

func prepareFullSnapshotInsertBlock(snapshotParts []model.DepthSnapshotPart) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var updateIdCol proto.ColInt64
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var symbCol proto.ColEnum
	for _, part := range snapshotParts {
		timestampCol.Append(time.UnixMilli(part.Timestamp))
		if part.T {
			typeCol.Append("bid")
		} else {
			typeCol.Append("ask")
		}
		priceCol.Append(part.Price)
		countCol.Append(part.Count)
		updateIdCol.Append(part.LastUpdateId)
		symbCol.Append(string(part.Symbol))
	}
	return proto.Input{
		{Name: TimestampCol, Data: timestampCol},
		{Name: "type", Data: &typeCol},
		{Name: "price", Data: &priceCol},
		{Name: "count", Data: &countCol},
		{Name: "last_update_id", Data: &updateIdCol},
		{Name: SymbolCol, Data: &symbCol},
	}
}

func (s ClickhouseRepo) SendDeltas(ctx context.Context, deltas []model.Delta) bool {
	if len(deltas) == 0 {
		return true
	}
	input := prepareDeltasInsertBlock(deltas)
	s.clientH.mut.Lock()
	err := s.clientH.client.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.DeltaTable),
		Input: input,
	})
	s.clientH.mut.Unlock()
	if err != nil {
		s.logger.Error(err.Error())
		return false
	}
	return true
}

func (s ClickhouseRepo) SendSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) bool {
	if len(snapshot) == 0 {
		return true
	}
	input := prepareFullSnapshotInsertBlock(snapshot)
	s.clientH.mut.Lock()
	err := s.clientH.client.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.SnapshotTable),
		Input: input,
	})
	s.clientH.mut.Unlock()
	if err != nil {
		s.logger.Error(err.Error())
		return false
	}
	return true
}

func (s ClickhouseRepo) GetLastSavedTimestamp(ctx context.Context, symb model.Symbol) time.Time {
	timestampResp := new(proto.ColDateTime64).WithPrecision(3)
	latestTimestamp := time.Unix(0, 0)
	s.clientH.mut.Lock()
	if err := s.clientH.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s WHERE %s = '%s' ORDER BY %s LIMIT 1",
			TimestampCol, s.cfg.DatabaseName, s.cfg.DeltaTable, SymbolCol, symb, TimestampCol),
		Result: proto.Results{
			{Name: TimestampCol, Data: timestampResp},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			if block.Rows != 0 {
				latestTimestamp = timestampResp.Data[0].Time(proto.PrecisionMax)
			}
			return nil
		},
	}); err != nil {
		s.logger.Error(err.Error())
	}
	s.clientH.mut.Unlock()
	return latestTimestamp
}

const (
	ExchangeInfoCol = "exchange_info"
	HashCol         = "hash"
)

func exInfoHash(exInfo *bmodel.ExchangeInfo) uint64 {
	payload, _ := json.Marshal(exInfo)
	return exInfoStringHash(string(payload))
}

func exInfoStringHash(s string) uint64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func (s ClickhouseRepo) prepareExchangeInfoInsertBlock(exInfo *bmodel.ExchangeInfo) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var exCol proto.ColStr
	var hashCol proto.ColUInt64
	timestampCol.Append(time.UnixMilli(exInfo.ServerTime))
	payload, err := json.Marshal(exInfo)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	exCol.Append(string(payload))
	hashCol.Append(exInfoStringHash(string(payload)))
	return proto.Input{
		{Name: TimestampCol, Data: timestampCol},
		{Name: ExchangeInfoCol, Data: &exCol},
		{Name: HashCol, Data: &hashCol},
	}
}

func (s ClickhouseRepo) SendFullExchangeInfoIfNeed(ctx context.Context, exInfo *bmodel.ExchangeInfo) bool {
	curHash := exInfoHash(exInfo)
	lastHash := s.GetLastFullExchangeInfoHash(ctx)
	if curHash == lastHash {
		s.logger.Info("Exchange info has not updated")
		return true
	}
	input := s.prepareExchangeInfoInsertBlock(exInfo)
	if input == nil {
		return false
	}
	s.clientH.mut.Lock()
	err := s.clientH.client.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.ExchangeInfoTable),
		Input: input,
	})
	s.clientH.mut.Unlock()
	if err != nil {
		s.logger.Error(err.Error())
		return false
	}
	s.logger.Info(fmt.Sprintf("successfully sended exchange info to Ch "))
	return true
}

func (s ClickhouseRepo) GetLastFullExchangeInfoHash(ctx context.Context) uint64 {
	var resp proto.ColStr
	var hash uint64
	s.clientH.mut.Lock()
	if err := s.clientH.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s ORDER BY %s LIMIT 1",
			HashCol, s.cfg.DatabaseName, s.cfg.ExchangeInfoTable, TimestampCol),
		Result: proto.Results{
			{Name: HashCol, Data: &resp},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			if block.Rows != 0 {
				var err error
				hash, err = strconv.ParseUint(resp.First(), 10, 64)
				if err != nil {
					s.logger.Warn(err.Error())
				}
			}
			return nil
		},
	}); err != nil {
		s.logger.Error(err.Error())
	}
	s.clientH.mut.Unlock()
	if hash == 0 {
		s.logger.Warn("empty get latest exchange info hash response")
	}
	return hash
}

func (s ClickhouseRepo) GetLastFullExchangeInfo(ctx context.Context) *bmodel.ExchangeInfo {
	var resp proto.ColStr
	var exInfo bmodel.ExchangeInfo
	s.clientH.mut.Lock()
	if err := s.clientH.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s ORDER BY %s LIMIT 1",
			ExchangeInfoCol, s.cfg.DatabaseName, s.cfg.ExchangeInfoTable, TimestampCol),
		Result: proto.Results{
			{Name: ExchangeInfoCol, Data: &resp},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			if block.Rows != 0 {
				if err := json.Unmarshal([]byte(resp.First()), &exInfo); err != nil {
					s.logger.Error(err.Error())
				}
			}
			return nil
		},
	}); err != nil {
		s.logger.Error(err.Error())
	}
	s.clientH.mut.Unlock()
	if exInfo.Timezone == "" {
		s.logger.Warn("empty get latest exchange info response")
	}
	return &exInfo
}

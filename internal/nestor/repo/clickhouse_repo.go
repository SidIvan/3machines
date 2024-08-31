package repo

import (
	"DeltaReceiver/internal/common/conf"
	model2 "DeltaReceiver/internal/nestor/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/clickhouse"
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
)

const (
	TimestampCol = "timestamp"
	SymbolCol    = "symbol"
)

type ClickhouseRepo struct {
	pool   *clickhouse.ChPoolHolder
	logger *zap.Logger
	cfg    *conf.GlobalRepoConfig
}

const (
	ChMaxConns = 45
	ChMinConns = 15
)

func NewClickhouseRepo(chPoolHolder *clickhouse.ChPoolHolder, cfg *conf.GlobalRepoConfig) *ClickhouseRepo {
	logger := log.GetLogger("ClickhouseRepo")
	return &ClickhouseRepo{
		logger: logger,
		pool:   chPoolHolder,
		cfg:    cfg,
	}
}

func prepareBookTickerInsertBlock(ticks []bmodel.SymbolTick) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var symbolCol proto.ColStr
	var bidPriceCol proto.ColStr
	var bidQuantityCol proto.ColStr
	var askPriceCol proto.ColStr
	var askQuantityCol proto.ColStr
	cutTime := time.Now()
	for _, tick := range ticks {
		timestampCol.Append(cutTime)
		symbolCol.Append(tick.Symbol)
		bidPriceCol.Append(tick.BidPrice)
		bidQuantityCol.Append(tick.BidQuantity)
		askPriceCol.Append(tick.AskPrice)
		askQuantityCol.Append(tick.AskQuantity)
	}
	return proto.Input{
		{Name: SymbolCol, Data: &symbolCol},
		{Name: TimestampCol, Data: timestampCol},
		{Name: "bid_price", Data: &bidPriceCol},
		{Name: "bid_quantity", Data: &bidQuantityCol},
		{Name: "ask_price", Data: &askPriceCol},
		{Name: "ask_quantity", Data: &askQuantityCol},
	}
}

func (s ClickhouseRepo) SendBookTicks(ctx context.Context, ticks []bmodel.SymbolTick) error {
	if s.pool == nil {
		return clickhouse.NilConnPool
	}
	if len(ticks) == 0 {
		s.logger.Warn("empty ticks slice")
		return nil
	}
	input := prepareBookTickerInsertBlock(ticks)
	err := s.pool.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.BookTickerTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending ticks %w", err)
	}
	return nil
}

func prepareFullSnapshotInsertBlock(snapshotParts []model2.DepthSnapshotPart) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var updateIdCol proto.ColInt64
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var symbCol proto.ColStr
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
		symbCol.Append(part.Symbol)
	}
	return proto.Input{
		{Name: SymbolCol, Data: &symbCol},
		{Name: TimestampCol, Data: timestampCol},
		{Name: "type", Data: &typeCol},
		{Name: "price", Data: &priceCol},
		{Name: "count", Data: &countCol},
		{Name: "last_update_id", Data: &updateIdCol},
	}
}

func (s ClickhouseRepo) SendSnapshot(ctx context.Context, snapshot []model2.DepthSnapshotPart) error {
	if s.pool == nil {
		return clickhouse.NilConnPool
	}
	if len(snapshot) == 0 {
		return nil
	}
	input := prepareFullSnapshotInsertBlock(snapshot)
	//s.logger.Debug(fmt.Sprintf("%d", s.pool.connPool))
	err := s.pool.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.SnapshotTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending snapshot %w", err)
	}
	return nil
}

const (
	ExchangeInfoCol = "exchange_info"
	HashCol         = "hash"
)

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
	hashCol.Append(exInfo.ExInfoHash())
	return proto.Input{
		{Name: TimestampCol, Data: timestampCol},
		{Name: ExchangeInfoCol, Data: &exCol},
		{Name: HashCol, Data: &hashCol},
	}
}

func (s ClickhouseRepo) SendFullExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) error {
	if s.pool == nil {
		return clickhouse.NilConnPool
	}
	curHash := exInfo.ExInfoHash()
	lastHash := s.GetLastFullExchangeInfoHash(ctx)
	//s.logger.Debug(fmt.Sprintf("%d", s.pool.connPool))
	if curHash == lastHash {
		s.logger.Info("Exchange info has not updated")
		return nil
	}
	input := s.prepareExchangeInfoInsertBlock(exInfo)
	err := s.pool.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.ExchangeInfoTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending exchange info %w", err)
	}
	s.logger.Info(fmt.Sprintf("successfully sended exchange info to Ch "))
	return nil
}

func (s ClickhouseRepo) GetLastFullExchangeInfoHash(ctx context.Context) uint64 {
	if s.pool == nil {
		return 0
	}
	var resp proto.ColUInt64
	var hash uint64
	if err := s.pool.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s ORDER BY %s DESC LIMIT 1",
			HashCol, s.cfg.DatabaseName, s.cfg.ExchangeInfoTable, TimestampCol),
		Result: proto.Results{
			{Name: HashCol, Data: &resp},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			if block.Rows != 0 {
				hash = resp.Row(0)
			}
			return nil
		},
	}); err != nil {
		s.logger.Error(err.Error())
	}
	if hash == 0 {
		s.logger.Warn("empty get latest exchange info hash response")
	}
	return hash
}

func (s ClickhouseRepo) GetLastFullExchangeInfo(ctx context.Context) *bmodel.ExchangeInfo {
	if s.pool == nil {
		return nil
	}
	var resp proto.ColStr
	var exInfo bmodel.ExchangeInfo
	if err := s.pool.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s ORDER BY %s DESC LIMIT 1",
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
	if exInfo.Timezone == "" {
		s.logger.Warn("empty get latest exchange info response")
	}
	return &exInfo
}

func (s ClickhouseRepo) Connect(ctx context.Context) error {
	return s.pool.Connect(ctx)
}

func (s ClickhouseRepo) Reconnect(ctx context.Context) error {
	return s.pool.Reconnect(ctx, 3)
}

func (s ClickhouseRepo) Disconnect(ctx context.Context) {
	s.pool.Disconnect()
}

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
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
	"time"
)

const (
	TimestampCol = "timestamp"
	SymbolCol    = "symbol"
)

type chClientHolder struct {
	connPool *chpool.Pool
	//mut      *sync.Mutex
}

//func (s *chClientHolder) SetNewClient(client *ch.Client) {
//	var x chpool.Pool
//	//s.mut.Lock()
//	if s.connPool != nil {
//		s.connPool.Close()
//	}
//	s.connPool, err := chpool.New(context.Background(), chpool.Options{})
//	//s.mut.Unlock()
//}

func (s *chClientHolder) Do(ctx context.Context, query ch.Query) error {
	//s.mut.Lock()
	//defer s.mut.Unlock()
	return s.connPool.Do(ctx, query)
}

type ClickhouseRepo struct {
	clientH *chClientHolder
	logger  *zap.Logger
	cfg     *conf.GlobalRepoConfig
}

func (s ClickhouseRepo) Reconnect(ctx context.Context) error {
	if connPool, err := chpool.Dial(ctx, chpool.Options{
		ClientOptions: ch.Options{
			Address: s.cfg.URI.GetAddress(),
		},
		MaxConns: 30,
		MinConns: 15,
	}); err != nil {
		s.logger.Error(err.Error())
		return err
	} else {
		s.clientH.connPool = connPool
		return err
	}
}

func NewClickhouseRepo(cfg *conf.GlobalRepoConfig) *ClickhouseRepo {
	logger := log.GetLogger("ClickhouseRepo")
	//var mutex sync.Mutex
	return &ClickhouseRepo{
		logger:  logger,
		clientH: &chClientHolder{
			//mut: &mutex,
		},
		cfg: cfg,
	}
}

func (s ClickhouseRepo) Connect(ctx context.Context) error {
	connPool, err := chpool.Dial(ctx, chpool.Options{
		ClientOptions: ch.Options{
			Address: s.cfg.URI.GetAddress(),
		},
		MaxConns: 30,
		MinConns: 15,
	})
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	s.clientH.connPool = connPool
	return nil
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
		{Name: TimestampCol, Data: timestampCol},
		{Name: "bid_price", Data: &bidPriceCol},
		{Name: "bid_quantity", Data: &bidQuantityCol},
		{Name: "ask_price", Data: &askPriceCol},
		{Name: "ask_quantity", Data: &askQuantityCol},
		{Name: SymbolCol, Data: &symbolCol},
	}
}

func (s ClickhouseRepo) SendBookTicks(ctx context.Context, ticks []bmodel.SymbolTick) error {
	if len(ticks) == 0 {
		s.logger.Warn("empty ticks slice")
		return nil
	}
	//s.logger.Debug(fmt.Sprintf("%d", s.clientH.connPool))
	input := prepareBookTickerInsertBlock(ticks)
	err := s.clientH.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.BookTickerTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending ticks %w", err)
	}
	return nil
}

func prepareDeltasInsertBlock(deltas []model.Delta) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColStr
	var countCol proto.ColStr
	var updateIdCol proto.ColInt64
	var firstUpdateIdCol proto.ColInt64
	var symbCol proto.ColStr
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
		{Name: TimestampCol, Data: timestampCol},
		{Name: "type", Data: &typeCol},
		{Name: "price", Data: &priceCol},
		{Name: "count", Data: &countCol},
		{Name: "last_update_id", Data: &updateIdCol},
		{Name: SymbolCol, Data: &symbCol},
	}
}

func (s ClickhouseRepo) SendDeltas(ctx context.Context, deltas []model.Delta) error {
	if len(deltas) == 0 {
		s.logger.Warn("empty deltas batch")
		return nil
	}
	input := prepareDeltasInsertBlock(deltas)
	//s.logger.Debug(fmt.Sprintf("%d", s.clientH.connPool))
	err := s.clientH.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.DeltaTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending deltas %w", err)
	}
	return nil
}

func (s ClickhouseRepo) SendSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	if len(snapshot) == 0 {
		return nil
	}
	input := prepareFullSnapshotInsertBlock(snapshot)
	//s.logger.Debug(fmt.Sprintf("%d", s.clientH.connPool))
	err := s.clientH.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.SnapshotTable),
		Input: input,
	})
	if err != nil {
		return fmt.Errorf("error while sending snapshot %w", err)
	}
	return nil
}

func (s ClickhouseRepo) GetLastSavedTimestamp(ctx context.Context, symb model.Symbol) time.Time {
	timestampResp := new(proto.ColDateTime64).WithPrecision(3)
	latestTimestamp := time.Unix(0, 0)
	if err := s.clientH.Do(ctx, ch.Query{
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
	return latestTimestamp
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

func (s ClickhouseRepo) Disconnect(ctx context.Context) {
	s.clientH.connPool.Close()
}

func (s ClickhouseRepo) SendFullExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) error {
	curHash := exInfo.ExInfoHash()
	lastHash := s.GetLastFullExchangeInfoHash(ctx)
	//s.logger.Debug(fmt.Sprintf("%d", s.clientH.connPool))
	if curHash == lastHash {
		s.logger.Info("Exchange info has not updated")
		return nil
	}
	input := s.prepareExchangeInfoInsertBlock(exInfo)
	err := s.clientH.Do(ctx, ch.Query{
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
	var resp proto.ColUInt64
	var hash uint64
	if err := s.clientH.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s ORDER BY %s LIMIT 1",
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
	var resp proto.ColStr
	var exInfo bmodel.ExchangeInfo
	if err := s.clientH.Do(ctx, ch.Query{
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
	if exInfo.Timezone == "" {
		s.logger.Warn("empty get latest exchange info response")
	}
	return &exInfo
}

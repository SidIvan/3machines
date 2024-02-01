package repo

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
	"time"
)

const (
	TimestampCol = "timestamp"
	SymbolCol    = "symbol"
)

type ClickhouseRepo struct {
	client *ch.Client
	logger *zap.Logger
	cfg    *conf.GlobalRepoConfig
}

func NewClickhouseRepo(cfg *conf.GlobalRepoConfig) *ClickhouseRepo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
	defer cancel()
	logger := log.GetLogger("ClickhouseRepo")
	client, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	return &ClickhouseRepo{
		logger: logger,
		client: client,
		cfg:    cfg,
	}
}

func prepareDeltasInsertBlock(deltas []model.Delta) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum
	var priceCol proto.ColFloat64
	var countCol proto.ColFloat64
	var updateIdCol proto.ColInt64
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
		symbCol.Append(string(delta.Symbol))
	}
	return proto.Input{
		{Name: TimestampCol, Data: timestampCol},
		{Name: "delta_type", Data: &typeCol},
		{Name: "price", Data: &priceCol},
		{Name: "count", Data: &countCol},
		{Name: "update_id", Data: &updateIdCol},
		{Name: SymbolCol, Data: &symbCol},
	}
}

func prepareFullSnapshotInsertBlock(snapshotParts []model.DepthSnapshotPart) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var updateIdCol proto.ColInt64
	var typeCol proto.ColEnum
	var priceCol proto.ColFloat64
	var countCol proto.ColFloat64
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
	input := prepareDeltasInsertBlock(deltas)
	for _, tmp := range input {
		fmt.Println(tmp.Data)
	}
	fmt.Println(input)
	err := s.client.Do(ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s.%s VALUES", s.cfg.DatabaseName, s.cfg.DeltaTable),
		Input: input,
	})
	if err != nil {
		s.logger.Error(err.Error())
		return false
	}
	return true
}

func (s ClickhouseRepo) SendSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) bool {
	input := prepareFullSnapshotInsertBlock(snapshot)
	err := s.client.Do(ctx, ch.Query{
		Body:  "INSERT INTO binance_full_snapshots VALUES",
		Input: input,
	})
	if err != nil {
		s.logger.Error(err.Error())
		return false
	}
	return true
}

func (s ClickhouseRepo) GetLastSavedTimestamp(ctx context.Context, symb model.Symbol) time.Time {
	timestampResp := new(proto.ColDateTime64).WithPrecision(3)
	latestTimestamp := time.Unix(0, 0)
	if err := s.client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("SELECT %s from %s.%s WHERE %s = %s ORDER BY %s LIMIT 1",
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

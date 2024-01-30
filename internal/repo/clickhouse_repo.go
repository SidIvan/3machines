package repo

import (
	"DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"context"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.uber.org/zap"
	"time"
)

type ClickhouseRepo struct {
	client *ch.Client
	logger *zap.Logger
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
	}
}

func prepareDeltasInsertBlock(deltas []model.Delta) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var typeCol proto.ColEnum8
	var priceCol proto.ColFloat64
	var countCol proto.ColFloat64
	var updateIdCol proto.ColInt64
	var symbCol proto.ColEnum8
	for _, delta := range deltas {
		timestampCol.Append(time.UnixMilli(delta.Timestamp))
		if delta.T {
			typeCol.Append(DeltaType2EnumValue["bid"])
		} else {
			typeCol.Append(DeltaType2EnumValue["ask"])
		}
		priceCol.Append(delta.Price)
		countCol.Append(delta.Count)
		updateIdCol.Append(delta.UpdateId)
		symbCol.Append(convSymbol2Enum(delta.Symbol))
	}
	return proto.Input{
		{Name: "timestamp", Data: timestampCol},
		{Name: "delta_type", Data: typeCol},
		{Name: "price", Data: priceCol},
		{Name: "count", Data: countCol},
		{Name: "update_id", Data: updateIdCol},
	}
}

func prepareFullSnapshotInsertBlock(snapshotParts []model.DepthSnapshotPart) proto.Input {
	timestampCol := new(proto.ColDateTime64).WithPrecision(3)
	var updateIdCol proto.ColInt64
	var typeCol proto.ColEnum8
	var priceCol proto.ColFloat64
	var countCol proto.ColFloat64
	var symbCol proto.ColEnum8
	for _, part := range snapshotParts {
		timestampCol.Append(time.UnixMilli(part.Timestamp))
		if part.T {
			typeCol.Append(DeltaType2EnumValue["bid"])
		} else {
			typeCol.Append(DeltaType2EnumValue["ask"])
		}
		priceCol.Append(part.Price)
		countCol.Append(part.Count)
		updateIdCol.Append(part.LastUpdateId)
		symbCol.Append(convSymbol2Enum(part.Symbol))
	}
	return proto.Input{
		{Name: "get_timestamp", Data: timestampCol},
		{Name: "delta_type", Data: typeCol},
		{Name: "price", Data: priceCol},
		{Name: "count", Data: countCol},
		{Name: "last_update_id", Data: updateIdCol},
	}
}

func (s ClickhouseRepo) SendDeltas(ctx context.Context, deltas []model.Delta) bool {
	input := prepareDeltasInsertBlock(deltas)
	err := s.client.Do(ctx, ch.Query{
		Body:  "INSERT INTO binance_deltas VALUES",
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

var DeltaType2EnumValue = map[string]proto.Enum8{
	"bid": 0,
	"ask": 1,
}

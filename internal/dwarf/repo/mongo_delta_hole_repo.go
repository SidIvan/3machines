package repo

import (
	"DeltaReceiver/internal/dwarf/cfg"
	"DeltaReceiver/internal/dwarf/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type MongoDeltaHoleStorage struct {
	logger        *zap.Logger
	DeltaHolesCol *mongo.Collection
	cfg           *cfg.HolesStorageConfig
	timeoutS      time.Duration
}

func NewMongoDeltaHoleStorage(cfg *cfg.HolesStorageConfig) *MongoDeltaHoleStorage {
	logger := log.GetLogger("MongoDeltaHoleStorage")
	return &MongoDeltaHoleStorage{
		logger:        logger,
		cfg:           cfg,
		DeltaHolesCol: &mongo.Collection{},
		timeoutS:      time.Duration(cfg.MongoConfig.TimeoutS) * time.Second,
	}
}

func (s MongoDeltaHoleStorage) Connect(ctx context.Context) error {
	s.logger.Debug("start connection to mongo")
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.MongoConfig.TimeoutS)*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(s.cfg.MongoConfig.URI.GetBaseUri()))
	if err != nil {
		return fmt.Errorf("error while connecting to mongo %w", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(s.cfg.MongoConfig.TimeoutS)*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("error while pinging to mongo %w", err)
	}
	s.logger.Debug("successfully connected to mongo")
	db := client.Database(s.cfg.MongoConfig.DatabaseName)
	*s.DeltaHolesCol = *db.Collection(s.cfg.DeltaHolesColName)
	return nil
}

func (s MongoDeltaHoleStorage) Reconnect(ctx context.Context) {
	s.logger.Debug("reconnecting to mongo")
	s.DeltaHolesCol.Database().Client().Disconnect(ctx)
	if err := s.Connect(ctx); err != nil {
		s.logger.Error(err.Error())
		return
	}
	s.logger.Debug("successfully reconnected to mongo")
}

func (s MongoDeltaHoleStorage) SaveDeltaHole(ctx context.Context, deltaHole *model.DeltaHoleWithInfo) error {
	_, err := s.DeltaHolesCol.InsertOne(ctx, deltaHole)
	if err != nil {
		err = fmt.Errorf("error while inserting delta hole %w", err)
	}
	return err
}

func (s MongoDeltaHoleStorage) GetDeltaHoles(ctx context.Context, fromTsMs, toTsMs int64) ([]model.DeltaHoleWithInfo, error) {
	s.logger.Debug(fmt.Sprintf("Get delta holes request from %d to %d", fromTsMs, toTsMs))
	filter := bson.M{"timestamp_ms": bson.M{"gte": fromTsMs, "lte": toTsMs}}
	cur, err := s.DeltaHolesCol.Find(ctx, filter)
	if err != nil {
		err = fmt.Errorf("error while getting delta holes %w", err)
		return nil, err
	}
	var deltaHoles []model.DeltaHoleWithInfo
	if err := cur.All(ctx, &deltaHoles); err != nil {
		err = fmt.Errorf("error while getting delta holes %w", err)
		return nil, err
	}
	return deltaHoles, nil
}

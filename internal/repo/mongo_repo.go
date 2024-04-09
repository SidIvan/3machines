package repo

import (
	appConf "DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

type LocalMongoRepo struct {
	logger             *zap.Logger
	BinanceDeltasCol   *mongo.Collection
	BinanceSnapshotCol *mongo.Collection
	ExInfoCol          *mongo.Collection
	BookTickerCol      *mongo.Collection
	cfg                *appConf.LocalRepoConfig
	timeoutS           time.Duration
}

func NewLocalMongoRepo(cfg *appConf.LocalRepoConfig) *LocalMongoRepo {
	logger := log.GetLogger("LocalMongoRepo")
	return &LocalMongoRepo{
		logger:   logger,
		cfg:      cfg,
		timeoutS: time.Duration(cfg.MongoConfig.TimeoutS) * time.Second,
	}
}

func (s LocalMongoRepo) Connect(ctx context.Context) error {
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
	s.BinanceSnapshotCol = db.Collection(s.cfg.SnapshotColName)
	s.BinanceDeltasCol = db.Collection(s.cfg.DeltaColName)
	s.ExInfoCol = db.Collection(s.cfg.ExInfoColName)
	s.BookTickerCol = db.Collection(s.cfg.BookTickerColName)
	return nil
}

func (s LocalMongoRepo) Reconnect(ctx context.Context) {
	s.logger.Debug("reconnecting to mongo")
	s.BookTickerCol.Database().Client().Disconnect(ctx)
	if err := s.Connect(ctx); err != nil {
		s.logger.Error(err.Error())
		return
	}
	s.logger.Debug("successfully reconnected to mongo")
}

func (s LocalMongoRepo) SaveDeltas(ctx context.Context, deltas []model.Delta) error {
	_, err := s.BinanceDeltasCol.InsertMany(ctx, []interface{}{deltas})
	if err != nil {
		err = fmt.Errorf("error while inserting deltas %w", err)
	}
	return err
}

func (s LocalMongoRepo) SaveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	_, err := s.BinanceSnapshotCol.InsertMany(ctx, []interface{}{snapshot})
	if err != nil {
		err = fmt.Errorf("error while inserting snapshot %w", err)
	}
	return err
}

func (s LocalMongoRepo) SaveExchangeInfo(ctx context.Context, exInfo *bmodel.ExchangeInfo) error {
	_, err := s.ExInfoCol.InsertOne(ctx, exInfo)
	if err != nil {
		err = fmt.Errorf("error while inserting exchange info %w", err)
	}
	return err
}

func (s LocalMongoRepo) SaveBookTicker(ctx context.Context, ticks []bmodel.SymbolTick) error {
	_, err := s.ExInfoCol.InsertMany(ctx, []interface{}{ticks})
	if err != nil {
		err = fmt.Errorf("error while inserting exchange info %w", err)
	}
	return err
}

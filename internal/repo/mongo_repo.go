package repo

import (
	appConf "DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		logger:             logger,
		cfg:                cfg,
		BinanceSnapshotCol: &mongo.Collection{},
		BinanceDeltasCol:   &mongo.Collection{},
		ExInfoCol:          &mongo.Collection{},
		BookTickerCol:      &mongo.Collection{},
		timeoutS:           time.Duration(cfg.MongoConfig.TimeoutS) * time.Second,
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
	*s.BinanceSnapshotCol = *db.Collection(s.cfg.SnapshotColName)
	*s.BinanceDeltasCol = *db.Collection(s.cfg.DeltaColName)
	*s.ExInfoCol = *db.Collection(s.cfg.ExInfoColName)
	*s.BookTickerCol = *db.Collection(s.cfg.BookTickerColName)
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
	_, err := s.BinanceDeltasCol.InsertMany(ctx, formDocuments(deltas))
	if err != nil {
		err = fmt.Errorf("error while inserting deltas %w", err)
	}
	return err
}

func (s LocalMongoRepo) GetDeltas(ctx context.Context, numDeltas int64) []model.DeltaWithId {
	cur, err := s.BinanceDeltasCol.Find(ctx, bson.M{}, &options.FindOptions{Limit: &numDeltas})
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	var results []model.DeltaWithId
	err = cur.All(ctx, &results)
	if err != nil {
		s.logger.Error(err.Error())
		return nil
	}
	return results
}

func (s LocalMongoRepo) DeleteDeltas(ctx context.Context, ids []primitive.ObjectID) (int64, error) {
	deleteRes, err := s.BinanceDeltasCol.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		s.logger.Error(err.Error())
		return 0, nil
	}
	return deleteRes.DeletedCount, nil
}

func (s LocalMongoRepo) SaveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) error {
	_, err := s.BinanceSnapshotCol.InsertMany(ctx, formDocuments(snapshot))
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
	_, err := s.ExInfoCol.InsertMany(ctx, formDocuments(ticks))
	if err != nil {
		err = fmt.Errorf("error while inserting ticks %w", err)
	}
	return err
}

func formDocuments[T any](entries []T) []any {
	var documents []interface{}
	for _, entry := range entries {
		documents = append(documents, entry)
	}
	return documents
}

package repo

import (
	appConf "DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"DeltaReceiver/pkg/mongo"
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

type LocalMongoRepo struct {
	logger             *zap.Logger
	BinanceDeltasCol   *mongo.Collection
	BinanceSnapshotCol *mongo.Collection
	cfg                *appConf.LocalRepoConfig
}

func NewLocalMongoRepo(cfg *appConf.LocalRepoConfig) *LocalMongoRepo {
	logger := log.GetLogger("LocalMongoRepo")
	repo := &LocalMongoRepo{
		logger: logger,
		cfg:    cfg,
	}
	repoRes := repo.initCollections(context.Background())
	repo = &repoRes
	return repo
}

func (s LocalMongoRepo) initCollections(ctx context.Context) LocalMongoRepo {
	if db, ok := mongo.ConnectMongo(s.logger, s.cfg.MongoConfig); ok {
		s.BinanceDeltasCol = mongo.NewMongoCollection(s.logger, s.cfg.MongoConfig, db.Collection(s.cfg.DeltaColName))
		s.BinanceSnapshotCol = mongo.NewMongoCollection(s.logger, s.cfg.MongoConfig, db.Collection(s.cfg.SnapshotColName))
	}
	return s
}

func (s LocalMongoRepo) Reconnect(ctx context.Context) {
	s.logger.Debug("reconnecting to mongo")
	s.initCollections(ctx)
	s.logger.Debug("successfully reconnected to mongo")
}

func (s LocalMongoRepo) SaveDeltas(ctx context.Context, deltas []model.Delta) bool {
	for _, delta := range deltas {
		if _, ok := s.BinanceDeltasCol.InsertDocument(delta); !ok {
			return false
		}
	}
	return true
}

func (s LocalMongoRepo) SaveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) bool {
	for _, snapshotPart := range snapshot {
		if _, ok := s.BinanceSnapshotCol.InsertDocument(snapshotPart); !ok {
			return false
		}
	}
	return true
}

func (s LocalMongoRepo) GetLastSavedTimestamp(ctx context.Context, symb model.Symbol) time.Time {
	delta := s.BinanceDeltasCol.GetAllWithFilterAndOptionsAndContext(ctx, bson.D{{"symb", symb}}, options.Find().SetSort(bson.D{{"timestamp", -1}}))
	if len(delta) == 0 {
		return time.Unix(0, 0)
	}
	return time.UnixMilli(delta[0].(model.Delta).Timestamp)
}

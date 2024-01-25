package repo

import (
	appConf "DeltaReceiver/internal/conf"
	"DeltaReceiver/internal/model"
	"DeltaReceiver/pkg/log"
	"DeltaReceiver/pkg/mongo"
	"context"
	"go.uber.org/zap"
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
	repo.initCollections(context.Background())
	return repo
}

func (s *LocalMongoRepo) initCollections(ctx context.Context) {
	if db, ok := mongo.ConnectMongo(s.logger, s.cfg.MongoConfig); !ok {
		s.BinanceDeltasCol = mongo.NewMongoCollection(s.logger, s.cfg.MongoConfig, db.Collection(s.cfg.DeltaColName))
		s.BinanceSnapshotCol = mongo.NewMongoCollection(s.logger, s.cfg.MongoConfig, db.Collection(s.cfg.SnapshotColName))
	}
}

func (s *LocalMongoRepo) Reconnect(ctx context.Context) {
	s.initCollections(ctx)
}

func (s *LocalMongoRepo) SaveDeltas(ctx context.Context, deltas []model.Delta) bool {
	for _, delta := range deltas {
		if _, ok := s.BinanceDeltasCol.InsertDocument(delta); !ok {
			return false
		}
	}
	return true
}

func (s *LocalMongoRepo) SaveSnapshot(ctx context.Context, snapshot []model.DepthSnapshotPart) bool {
	for _, snapshotPart := range snapshot {
		if _, ok := s.BinanceSnapshotCol.InsertDocument(snapshotPart); !ok {
			return false
		}
	}
	return true
}

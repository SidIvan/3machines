package mongo

import (
	"DeltaReceiver/pkg/mongo/conf"
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

type Collection struct {
	log     *zap.Logger
	timeout time.Duration
	cfg     *conf.MongoRepoConfig
	col     *mongo.Collection
}

func NewMongoCollection(log *zap.Logger, cfg *conf.MongoRepoConfig, col *mongo.Collection) *Collection {
	return &Collection{
		log:     log,
		timeout: time.Duration(cfg.TimeoutS),
		cfg:     cfg,
		col:     col,
	}
}

func ConnectMongo(logger *zap.Logger, cfg *conf.MongoRepoConfig) (*mongo.Database, bool) {
	return ConnectMongoWithContext(context.Background(), logger, cfg)
}

func ConnectMongoWithContext(ctx context.Context, logger *zap.Logger, cfg *conf.MongoRepoConfig) (*mongo.Database, bool) {
	for i := int8(0); i < cfg.NumConnRetries; i++ {
		if database, ok := SingleConnectMongoWithContext(ctx, logger, cfg); ok {
			return database, ok
		}
	}
	return nil, false
}

func SingleConnectMongoWithContext(ctx context.Context, logger *zap.Logger, cfg *conf.MongoRepoConfig) (*mongo.Database, bool) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutS)*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI.GetBaseUri()))
	if err != nil {
		logger.Error(err.Error())
		return nil, false
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		logger.Error(err.Error())
		return nil, false
	}
	return client.Database(cfg.DatabaseName), true

}

func (s *Collection) GetAll() []interface{} {
	return s.GetAllWithContext(context.Background())
}

func (s *Collection) GetAllWithContext(ctx context.Context) []interface{} {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	cur, err := s.col.Find(ctx, bson.M{})
	if err != nil {
		s.log.Error(err.Error())
		return nil
	}
	ctx, cancel = context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var documents []interface{}
	err = cur.All(ctx, &documents)
	if err != nil {
		s.log.Error(err.Error())
		return nil
	}
	return documents
}

func (s *Collection) InsertDocument(doc interface{}) (interface{}, bool) {
	return s.InsertDocumentWithContext(context.Background(), doc)
}

func (s *Collection) InsertDocumentWithContext(ctx context.Context, doc interface{}) (interface{}, bool) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.col.InsertOne(ctx, doc)
	if err != nil {
		s.log.Error(err.Error())
		return nil, false
	}
	return res.InsertedID, true
}

func (s *Collection) UpdateSingleDocument(filter bson.D, update bson.D) bool {
	return s.UpdateSingleDocumentWithContext(context.Background(), filter, update)
}

func (s *Collection) UpdateSingleDocumentWithContext(ctx context.Context, filter bson.D, update bson.D) bool {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.col.UpdateOne(ctx, filter, update)
	if err != nil {
		s.log.Error(err.Error())
		return false
	}
	if res.ModifiedCount != 1 {
		s.log.Error(UpdateErr.Error())
		return false
	}
	return true
}

func (s *Collection) DeleteSingleDocument(filter bson.D) bool {
	return s.DeleteSingleDocumentWithContext(context.Background(), filter)
}

func (s *Collection) DeleteSingleDocumentWithContext(ctx context.Context, filter bson.D) bool {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.col.DeleteOne(ctx, filter)
	if err != nil {
		s.log.Error(err.Error())
		return false
	}
	if res.DeletedCount != 1 {
		s.log.Error(DeleteErr.Error())
		return false
	}
	return true
}

func (s *Collection) DeleteManyDocuments(filter bson.D) (int64, bool) {
	return s.DeleteManyDocumentsWithContex(context.Background(), filter)
}

func (s *Collection) DeleteManyDocumentsWithContex(ctx context.Context, filter bson.D) (int64, bool) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.col.DeleteMany(ctx, filter)
	if err != nil {
		s.log.Error(err.Error())
		return 0, false
	}
	return res.DeletedCount, true
}

var (
	UpdateErr = errors.New("update was not success")
	DeleteErr = errors.New("delete was not success")
)

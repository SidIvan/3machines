package repo

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var (
	fixBatchSize int64 = 1000
)

type LocalMongoRepo[T any, TM model.WithMongoIdData[T]] struct {
	logger     *zap.Logger
	collection *mongo.Collection
	timeoutS   time.Duration
}

func NewLocalMongoRepo[T any, TM model.WithMongoIdData[T]](timeoutS int, collectionName string, db *mongo.Database) *LocalMongoRepo[T, TM] {
	logger := log.GetLogger("LocalMongoRepo")
	return &LocalMongoRepo[T, TM]{
		logger:     logger,
		collection: db.Collection(collectionName),
		timeoutS:   time.Duration(timeoutS) * time.Second,
	}
}

func (s LocalMongoRepo[T, TM]) Save(ctx context.Context, batch []T) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, s.timeoutS)
	defer cancel()
	_, err := s.collection.InsertMany(ctxWithTimeout, formDocuments(batch))
	if err != nil {
		err = fmt.Errorf("error while inserting deltas %w", err)
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

func (s LocalMongoRepo[T, TM]) GetWithDeleteCallback(ctx context.Context) ([]T, error, func() error) {
	cur, err := s.collection.Find(ctx, bson.M{}, &options.FindOptions{Limit: &fixBatchSize})
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err, func() error { return nil }
	}
	var results []TM
	err = cur.All(ctx, &results)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err, func() error { return nil }
	}
	data := make([]T, len(results))
	mongoIds := make([]primitive.ObjectID, len(results))
	for i, row := range results {
		data[i] = row.ToData()
		mongoIds[i] = row.MongoId()
	}
	return data, nil, func() error {
		return s.deleteData(ctx, mongoIds)
	}
}

func (s LocalMongoRepo[T, TM]) deleteData(ctx context.Context, ids []primitive.ObjectID) error {
	_, err := s.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

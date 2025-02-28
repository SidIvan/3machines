package model

import "go.mongodb.org/mongo-driver/bson/primitive"

type WithTimestampMs interface {
	GetTimestampMs() int64
}

type WithSymbol interface {
	GetSymbol() string
}

type BinanceDataRow interface {
	WithTimestampMs
	WithSymbol
}

type WithMongoIdData[T any] interface {
	MongoId() primitive.ObjectID
	ToData() T
}

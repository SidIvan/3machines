package model

import (
	bmodel "DeltaReceiver/pkg/binance/model"
	"DeltaReceiver/pkg/log"
	"encoding/json"
	"reflect"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

var logger *zap.Logger = log.GetLogger("Global")

type ExchangeInfo struct {
	ServerTime int64  `json:"timestamp" bson:"timestamp" parquet:"timestampMs"`
	ExInfoHash int64  `json:"hash" bson:"hash" parquet:"hash"`
	Payload    string `json:"payload" bson:"payload" parquet:"payload"`
}

type ExchangeInfoWithMongoId struct {
	ServerTime int64              `json:"timestamp" bson:"timestamp" parquet:"timestampMs"`
	ExInfoHash int64              `json:"hash" bson:"hash" parquet:"hash"`
	Payload    string             `json:"payload" bson:"payload" parquet:"payload"`
	Id         primitive.ObjectID `bson:"_id"`
}

func NewExchangeInfo(rawExInfo *bmodel.ExchangeInfo) *ExchangeInfo {
	payload, err := json.Marshal(rawExInfo)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	return &ExchangeInfo{
		ServerTime: rawExInfo.ServerTime,
		ExInfoHash: rawExInfo.ExInfoHash(),
		Payload:    string(payload),
	}
}

func (s ExchangeInfoWithMongoId) MongoId() primitive.ObjectID {
	return s.Id
}

func (s ExchangeInfoWithMongoId) ToData() ExchangeInfo {
	return ExchangeInfo{
		ServerTime: s.ServerTime,
		ExInfoHash: s.ExInfoHash,
		Payload:    s.Payload,
	}
}

func EqualsExchangeInfos(info1 *ExchangeInfo, info2 *bmodel.ExchangeInfo) bool {
	var info11 bmodel.ExchangeInfo
	_ = json.Unmarshal([]byte(info1.Payload), &info11)
	return info11.Timezone == info2.Timezone &&
		reflect.DeepEqual(info11.ExchangeFilters, info2.ExchangeFilters) &&
		reflect.DeepEqual(info11.RateLimits, info2.RateLimits) &&
		reflect.DeepEqual(info11.Symbols, info2.Symbols)
}

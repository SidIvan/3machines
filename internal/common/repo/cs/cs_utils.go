package cs

import (
	"DeltaReceiver/internal/common/model"
	"time"
)

var epochStart = time.Unix(0, 0).UTC()

func getDayNo(timestampMs int64) int64 {
	return int64(GetHourNo(timestampMs) / 24)
}

func GetHourNo(timestampMs int64) int64 {
	timestamp := time.UnixMilli(timestampMs).UTC()
	days := timestamp.Sub(epochStart).Hours()
	return int64(days)
}

func SplitDataToBatches[T model.BinanceDataRow](data []T) map[model.ProcessingKey][]T {
	keyToBatch := make(map[model.ProcessingKey][]T)
	for _, row := range data {
		key := model.ProcessingKey{
			Symbol: row.GetSymbol(),
			HourNo: GetHourNo(row.GetTimestampMs()),
		}
		keyToBatch[key] = append(keyToBatch[key], row)
	}
	return keyToBatch
}

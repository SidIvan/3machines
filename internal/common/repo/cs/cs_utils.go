package cs

import "time"

var epochStart = time.Unix(0, 0).UTC()

func getDayNo(timestampMs int64) int64 {
	return int64(getHourNo(timestampMs) / 24) 
}

func getHourNo(timestampMs int64) int64 {
	timestamp := time.UnixMilli(timestampMs).UTC()
	days := timestamp.Sub(epochStart).Hours()
	return int64(days) 
}
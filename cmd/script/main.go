package main

import (
	"DeltaReceiver/internal/common/model"

	"github.com/segmentio/parquet-go"
)

func main() {
	writer := parquet.NewGenericWriter[model.Delta]()
}

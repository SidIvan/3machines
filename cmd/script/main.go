package main

import (
	"DeltaReceiver/internal/common/model"
	"fmt"
	"os"

	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {
	ts := time.Now()
	fmt.Println(ts)
	keyDate := ts.Format("2006-01-02")
	keyTime := ts.Format("15-04-05")
	fmt.Println(fmt.Sprintf("%s/%s/%s/%s.parquet", "binance/deltas", "BTCUSDT", keyDate, keyTime))
}

func pqtWrite() {
	filename := "my_empty_file.txt"
	file, _ := os.Create(filename)
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = &parquet.Lz4Raw
	writer := parquet.NewGenericWriter[model.Delta](file, cfg)
	schema := parquet.SchemaOf(new(model.Delta))
	fmt.Println(schema)
	writer.Write([]model.Delta{
		{
			Timestamp:     123,
			Price:         "asd",
			Count:         "qwe",
			T:             true,
			UpdateId:      13,
			FirstUpdateId: 13,
			Symbol:        "ABC",
		},
		{
			Timestamp:     123,
			Price:         "asd",
			Count:         "qwe",
			T:             true,
			UpdateId:      13,
			FirstUpdateId: 13,
			Symbol:        "ABC",
		},
		{
			Timestamp:     123,
			Price:         "asd",
			Count:         "qwe",
			T:             true,
			UpdateId:      13,
			FirstUpdateId: 13,
			Symbol:        "ABC",
		},
	})
	writer.Close()
}

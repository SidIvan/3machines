package main

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/binance"
	"DeltaReceiver/pkg/conf"
	"context"
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	x := binance.NewBookTickerClient(&binance.BinanceHttpClientConfig{
		StreamBaseUriConfig: &conf.BaseUriConfig{
			Schema:   "ws://",
			Host:     "147.45.237.160",
			Port:     8123,
			BasePath: "/stream",
		},
		UseAllTickersStream: false,
	},
		[]string{"btcusdt"})

	x.ConnectWs(context.Background())
	msgs := 0
	for {
		_, e := x.Recv(context.Background())
		if e != nil {
			panic(e)
		}
		msgs++
		if msgs%1000 == 0 {
			fmt.Printf("got %d msgs\n", msgs)
		}
	}
	// ts := time.Now()
	// fmt.Println(ts)
	// keyDate := ts.Format("2006-01-02")
	// keyTime := ts.Format("15-04-05")
	// fmt.Println(fmt.Sprintf("%s/%s/%s/%s.parquet", "binance/deltas", "BTCUSDT", keyDate, keyTime))
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

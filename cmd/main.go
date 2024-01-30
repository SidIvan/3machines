package main

import (
	"DeltaReceiver/internal/app"
	"DeltaReceiver/internal/conf"
	"context"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfgData, err := os.ReadFile("cmd/test.yaml")
	if err != nil {
		log.Println(err.Error())
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	var cfg conf.AppConfig
	yaml.Unmarshal(cfgData, &cfg)
	a := app.NewApp(&cfg)
	a.Start()
	<-ctx.Done()
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(15)*time.Second)
	a.Stop(ctx)
}

package main

import (
	"DeltaReceiver/internal/nestor/app"
	"DeltaReceiver/internal/nestor/conf"
	"context"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// cfgPath := flag.String("cfg", "nestor.yaml", "path to config file")
	// flag.Parse()
	// cfgData, err := os.ReadFile(*cfgPath)
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return
	// }
	// var cfg conf.AppConfig
	// if err = yaml.Unmarshal(cfgData, &cfg); err != nil {
	// 	log.Println(err.Error())
	// 	return
	// }
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	cfg := conf.NewAppConfigFromEnv("nestor")
	a := app.NewApp(cfg)
	a.Start()
	<-ctx.Done()
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancel()
	a.Stop(ctx)
}

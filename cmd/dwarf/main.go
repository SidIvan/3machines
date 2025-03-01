package main

import (
	"DeltaReceiver/internal/dwarf/app"
	"DeltaReceiver/internal/dwarf/cfg"
	"context"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	cfg := cfg.NewAppConfigFromEnv()
	a := app.NewApp(cfg)
	a.Start()
	<-ctx.Done()
	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancel()
	a.Stop(ctx)
}

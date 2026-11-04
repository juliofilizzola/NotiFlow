package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/juliofilizzola/notiflow/internal/app"
	"github.com/juliofilizzola/notiflow/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.New(config.Load(), log).Run(ctx); err != nil {
		log.Error("notiflow failed", "err", err)
		os.Exit(1)
	}
}

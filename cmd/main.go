package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/taskmarket/indexer/internal/config"
	"github.com/taskmarket/indexer/internal/db"
	"github.com/taskmarket/indexer/internal/indexer"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config error", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gdb, err := db.Connect(cfg.DatabaseURL)
	if err != nil {
		slog.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close(gdb)

	chains, err := config.LoadChains(ctx, gdb)
	if err != nil {
		slog.Error("config error", "error", err)
		os.Exit(1)
	}

	idx, err := indexer.New(cfg, chains, gdb)
	if err != nil {
		slog.Error("indexer init failed", "error", err)
		os.Exit(1)
	}

	chainIDs := make([]int64, 0, len(chains))
	for _, c := range chains {
		chainIDs = append(chainIDs, c.ChainID)
	}
	slog.Info("TaskMarket Go indexer starting",
		"chains", chainIDs,
		"poll_ms", cfg.PollIntervalMs,
	)

	if err := idx.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("indexer exited with error", "error", err)
		os.Exit(1)
	}

	slog.Info("indexer stopped cleanly")
}

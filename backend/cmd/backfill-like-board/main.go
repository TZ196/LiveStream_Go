// 维护窗口入口：两个消费组追平并退出后，从 MySQL 的唯一事件补建各场排行榜。
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		slog.Error("leaderboard_backfill_failed", "error", err)
		os.Exit(1)
	}
	slog.Info("leaderboard_backfill_complete")
}

func run(ctx context.Context) error {
	cfg, err := config.LoadMySQL()
	if err != nil {
		return err
	}
	likesConfig, err := config.LoadLikes()
	if err != nil {
		return err
	}
	if !likesConfig.Enabled {
		return errors.New("请配置并启用点赞链路")
	}
	db, err := database.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer database.Close(db)
	if err = database.CheckSchema(ctx, db); err != nil {
		return err
	}
	client := redis.NewClient(&redis.Options{Addr: likesConfig.RedisAddr})
	defer client.Close()
	repo, err := likes.NewProjectionRepository(client, likesConfig.ProjectionEpoch)
	if err != nil {
		return err
	}
	return likes.NewProjectionInitializer(db, repo).BackfillLeaderboards(ctx, likesConfig.Brokers)
}

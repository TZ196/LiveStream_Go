package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/TZ196/LiveStream_Go/backend/internal/logging"
)

// main 启动点赞持久化 Worker，并按信号结束消费。
func main() {
	logger := logging.New(os.Stdout, slog.LevelInfo).With("service", "worker")
	if err := run(); err != nil {
		logger.Error("worker_failed", "error", err)
		os.Exit(1)
	}
}

// run 检查配置和数据库，再运行独立的 Kafka 持久化消费组。
func run() error {
	cfg, err := config.LoadMySQL()
	if err != nil {
		return err
	}
	likesConfig, err := config.LoadLikes()
	if err != nil {
		return err
	}
	if !likesConfig.Enabled {
		return errors.New("请启用点赞链路并配置 Kafka")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	db, err := database.Open(startup, cfg)
	if err != nil {
		return err
	}
	defer database.Close(db)
	if err := database.CheckSchema(startup, db); err != nil {
		return err
	}
	repository := likes.NewPersistenceRepository(db)
	for ctx.Err() == nil {
		check, checkCancel := context.WithTimeout(ctx, 10*time.Second)
		consumer, consumerErr := likes.OpenConsumerGroup(check, likesConfig.Brokers, "live-persistence", likesConfig.PersistenceConsumers, repository.ValidatePersistenceStartup)
		checkCancel()
		if consumerErr == nil {
			consumerErr = consumer.Run(ctx, repository.PersistLike)
			consumer.Close()
		}
		if ctx.Err() != nil {
			return nil
		}
		slog.Error("persistence_consumer_paused", "error", consumerErr)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}

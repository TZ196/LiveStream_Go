package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/TZ196/LiveStream_Go/backend/internal/logging"
)

func main() {
	logger := logging.New(os.Stdout, slog.LevelInfo).With("service", "worker")
	if err := run(); err != nil {
		logger.Error("worker_failed", "error", err)
		os.Exit(1)
	}
}

// run 只组装共享连接池和两条独立持久化循环，业务失败不能取消整个进程。
func run() error {
	cfg, err := config.LoadMySQL()
	if err != nil {
		return err
	}
	likeConfig, err := config.LoadLikes()
	if err != nil {
		return err
	}
	danmakuConfig, err := config.LoadDanmaku()
	if err != nil {
		return err
	}
	if !likeConfig.Enabled && !danmakuConfig.Enabled {
		return errors.New("请启用点赞或弹幕持久化")
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
	metrics := danmaku.NewMetrics()
	stopMetrics := startWorkerMetrics(metrics)
	defer stopMetrics()
	var workers sync.WaitGroup
	if likeConfig.Enabled {
		repository := likes.NewPersistenceRepository(db)
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				check, cancel := context.WithTimeout(ctx, 15*time.Second)
				group, err := likes.OpenConsumerGroup(check, likeConfig.Brokers, "live-persistence", likeConfig.PersistenceConsumers, repository.ValidatePersistenceStartup)
				cancel()
				if err == nil {
					metrics.Consumer.WithLabelValues("live-persistence").Set(1)
					slog.Info("persistence_consumer_running", "group", "live-persistence", "state", "RUNNING")
					err = group.Run(ctx, repository.PersistLike)
					group.Close()
				}
				metrics.Consumer.WithLabelValues("live-persistence").Set(0)
				if ctx.Err() != nil {
					return
				}
				slog.Error("persistence_consumer_paused", "group", "live-persistence", "error", err)
				if errors.Is(err, likes.ErrRecoveryBlocked) {
					<-ctx.Done()
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
			}
		}()
	}
	if danmakuConfig.Enabled {
		repository := danmaku.NewRepository(db)
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				check, cancel := context.WithTimeout(ctx, 15*time.Second)
				group, err := danmaku.OpenConsumerGroup(check, danmakuConfig.Brokers, danmaku.PersistenceGroup, danmakuConfig.PersistenceConsumers, repository.ValidateCheckpoint)
				cancel()
				if err == nil {
					slog.Info("persistence_consumer_running", "group", danmaku.PersistenceGroup, "state", "RUNNING")
					err = group.RunObserved(ctx, func(ctx context.Context, record danmaku.Record) error {
						err := repository.Persist(ctx, record)
						if err == nil && !record.ProducedAt.IsZero() && time.Now().After(record.ProducedAt) {
							metrics.Persist.Observe(time.Since(record.ProducedAt).Seconds())
						}
						return err
					}, repository.Quarantine, metrics)
					group.Close()
				}
				if ctx.Err() != nil {
					return
				}
				slog.Error("persistence_consumer_paused", "group", danmaku.PersistenceGroup, "error", err)
				if errors.Is(err, danmaku.ErrBlocked) || errors.Is(err, danmaku.ErrConflict) {
					<-ctx.Done()
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
			}
		}()
	}
	<-ctx.Done()
	workers.Wait()
	return nil
}

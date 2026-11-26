package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/TZ196/LiveStream_Go/backend/internal/httpserver"
	"github.com/TZ196/LiveStream_Go/backend/internal/realtime"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// startDanmaku 失败只关闭弹幕准入；已接受事件的广播、持久化与审核器分别运行。
func startDanmaku(parent context.Context, db *gorm.DB, rooms *room.Service, users *user.Repository, manager *realtime.Manager, api *httpserver.Server, logger *slog.Logger) func() {
	cfg, err := config.LoadDanmaku()
	if err != nil {
		logger.Error("danmaku_config_invalid", "error", err)
		return func() {}
	}
	if !cfg.Enabled {
		return func() {}
	}
	moderator, err := danmaku.LoadModerator(cfg.WordsPath)
	if err != nil {
		logger.Error("danmaku_words_unavailable", "error", err)
	}
	producer, err := danmaku.NewProducer(cfg.Brokers)
	if err != nil {
		logger.Error("danmaku_producer_unavailable", "error", err)
		return func() {}
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	repository := danmaku.NewRepository(db)
	recent := danmaku.NewRecentStore(client)
	service := danmaku.NewService(danmaku.NewRequestStore(client), repository, moderator, producer, rooms, users)
	manager.EnableDanmaku(service)
	api.EnableDanmakuHistory(repository)
	ctx, cancel := context.WithCancel(parent)
	recovery := danmaku.NewRecentRecovery(ctx, recent, repository, cfg.Brokers)
	api.EnableDanmakuRecent(recent, recovery)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			check, done := context.WithTimeout(ctx, 3*time.Second)
			history, err := repository.HasHistory(check)
			if err == nil {
				var logs bool
				logs, err = producer.HasEvents(check)
				history = history || logs
			}
			if err == nil {
				err = service.Store.Check(check, history)
			}
			done()
			service.SetHealthy(err == nil)
			if err != nil && ctx.Err() == nil {
				logger.Warn("danmaku_dependencies_unavailable", "error", err)
			}
			select {
			case <-ctx.Done():
				service.SetHealthy(false)
				return
			case <-ticker.C:
			}
		}
	}()
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			check, done := context.WithTimeout(ctx, 15*time.Second)
			group, err := danmaku.OpenConsumerGroup(check, cfg.Brokers, danmaku.RealtimeGroup, cfg.RealtimeConsumers, service.Store.ValidateRealtime)
			done()
			if err == nil {
				service.SetConsuming(true)
				logger.Info("danmaku_realtime_running")
				err = group.RunObserved(ctx, func(ctx context.Context, record danmaku.Record) error {
					if err := manager.QueueDanmaku(ctx, record); err != nil {
						return err
					}
					return service.Store.MarkRealtime(ctx, record)
				}, func(ctx context.Context, failed danmaku.FailedRecord) error {
					// 实时组先独立落库隔离，再推进自己的 Redis 进度，最后由消费循环提交 Kafka。
					if err := repository.Quarantine(ctx, failed); err != nil {
						return err
					}
					position := danmaku.Record{TopicID: failed.TopicID, Partition: failed.Raw.Partition, NextOffset: failed.Raw.Offset + 1}
					return service.Store.MarkRealtime(ctx, position)
				}, service.Metrics)
				group.Close()
			}
			service.SetConsuming(false)
			if ctx.Err() != nil {
				return
			}
			logger.Error("danmaku_realtime_paused", "error", err)
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
	go func() {
		defer workers.Done()
		for ctx.Err() == nil {
			check, done := context.WithTimeout(ctx, 15*time.Second)
			group, err := danmaku.OpenConsumerGroup(check, cfg.Brokers, danmaku.RecentCacheGroup, cfg.RealtimeConsumers, func(context.Context, string, map[int32]int64) error { return nil })
			done()
			if err == nil {
				err = group.RunObserved(ctx, recent.Add, func(_ context.Context, failed danmaku.FailedRecord) error {
					logger.Warn("danmaku_recent_invalid", "partition", failed.Raw.Partition, "offset", failed.Raw.Offset)
					return nil
				}, service.Metrics)
				group.Close()
			}
			if ctx.Err() != nil {
				return
			}
			logger.Error("danmaku_recent_paused", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
	return func() { cancel(); workers.Wait(); recovery.Close(); producer.Close(); _ = client.Close() }
}

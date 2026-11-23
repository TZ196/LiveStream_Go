package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/httpserver"
	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
	"github.com/TZ196/LiveStream_Go/backend/internal/realtime"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/gorm"
)

// startLikes 可选启动实时消费和 WebSocket；返回的关闭函数等待后台任务结束。
func startLikes(parent context.Context, db *gorm.DB, rooms *room.Service, api *httpserver.Server, logger *slog.Logger) (func(), error) {
	cfg, err := config.LoadLikes()
	if err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return func() {}, nil
	}
	producer, err := messaging.NewProducer(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	startup, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err = redisClient.Ping(startup).Err(); err != nil {
		producer.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("连接 Redis: %w", err)
	}
	projection, err := messaging.NewProjectionRepository(redisClient, cfg.ProjectionEpoch)
	if err != nil {
		producer.Close()
		_ = redisClient.Close()
		return nil, err
	}
	initializer := messaging.NewProjectionInitializer(db, projection)
	consumer, err := messaging.OpenConsumerGroup(startup, cfg.Brokers, "live-realtime", cfg.RealtimeConsumers, initializer.ValidateProjectionStartup)
	if err != nil {
		producer.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("实时投影检查: %w", err)
	}
	manager := realtime.NewManager(rooms, producer, projection, initializer, user.NewRepository(db), cfg.ProjectionEpoch)
	api.EnableRealtime(manager, cfg.WSOrigin)
	manager.SetAvailable(true)
	ctx, stop := context.WithCancel(parent)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() { defer wait.Done(); manager.RunLeaderboard(ctx) }()
	go func() {
		defer wait.Done()
		manager.RunLikeUpdates(ctx)
		// 先终止长连接读循环，Hertz 才能在关闭预算内回收升级后的请求。
		manager.Close()
	}()
	go func() {
		defer wait.Done()
		current := consumer
		for {
			if ctx.Err() != nil {
				current.Close()
				return
			}
			err := current.Run(ctx, func(ctx context.Context, record *kgo.Record, _ string) error {
				event, err := messaging.DecodeLikeRecord(record)
				if err != nil {
					return err
				}
				if err = initializer.Ensure(ctx, event); err != nil {
					return err
				}
				result, err := projection.ApplyLike(ctx, record)
				if err != nil {
					return err
				}
				return manager.QueueLikeUpdate(ctx, event, result)
			})
			current.Close()
			if ctx.Err() != nil {
				return
			}
			manager.SetAvailable(false)
			logger.Error("realtime_consumer_paused", "error", err)
			manager.BroadcastUnavailable()
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
				check, cancel := context.WithTimeout(ctx, 10*time.Second)
				next, createErr := messaging.OpenConsumerGroup(check, cfg.Brokers, "live-realtime", cfg.RealtimeConsumers, initializer.ValidateProjectionStartup)
				cancel()
				if createErr == nil {
					current = next
					manager.Resume()
					break
				}
				logger.Error("realtime_consumer_waiting", "error", createErr)
			}
		}
	}()
	return func() {
		stop()
		manager.Close()
		wait.Wait()
		producer.Close()
		_ = redisClient.Close()
	}, nil
}

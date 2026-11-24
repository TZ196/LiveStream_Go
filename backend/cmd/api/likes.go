package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/TZ196/LiveStream_Go/backend/internal/realtime"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// startLikes 只组装点赞依赖并启动检查/恢复，共享连接由 API main 管理。
func startLikes(parent context.Context, db *gorm.DB, manager *realtime.Manager, logger *slog.Logger) (func(), error) {
	cfg, err := config.LoadLikes()
	if err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return func() {}, nil
	}
	producer, err := likes.NewProducer(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	initial, err := likes.NewProjectionRepository(redisClient, cfg.ProjectionEpoch)
	if err != nil {
		producer.Close()
		_ = redisClient.Close()
		return nil, err
	}
	manager.ConfigureLikes(producer, initial, likes.NewProjectionInitializer(db, initial))
	recovery := likes.NewRecovery(db, redisClient, cfg.Brokers, cfg.ProjectionEpoch)
	ctx, stop := context.WithCancel(parent)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() { defer wait.Done(); manager.RunLeaderboard(ctx) }()
	go func() { defer wait.Done(); manager.RunLikeUpdates(ctx) }()
	go func() { defer wait.Done(); runRealtimeConsumer(ctx, cfg, recovery, producer, manager, logger) }()
	return func() {
		stop()
		wait.Wait()
		producer.Close()
		_ = redisClient.Close()
	}, nil
}

// runRealtimeConsumer 串行管理检查、必要的重建和消费，不运行多个恢复任务。
func runRealtimeConsumer(ctx context.Context, cfg config.Likes, recovery *likes.Recovery, producer *likes.Producer, manager *realtime.Manager, logger *slog.Logger) {
	for ctx.Err() == nil {
		manager.Pause("CHECKING")
		check, cancel := context.WithTimeout(ctx, 30*time.Second)
		projection, initializer, err := recovery.Check(check)
		cancel()
		if errors.Is(err, likes.ErrProjectionRecovery) {
			manager.Pause("RECOVERING")
			rebuild, finish := context.WithTimeout(ctx, 2*time.Minute)
			err = producer.Drain(rebuild)
			if err == nil {
				projection, initializer, err = recovery.Rebuild(rebuild)
			}
			finish()
			if err == nil {
				logger.Info("likes_projection_rebuilt", "epoch", projection.Epoch())
			}
		}
		if err == nil {
			startup, finish := context.WithTimeout(ctx, 10*time.Second)
			var consumer *likes.ConsumerGroup
			consumer, err = likes.OpenConsumerGroup(startup, cfg.Brokers, "live-realtime", cfg.RealtimeConsumers, initializer.ValidateProjectionStartup)
			finish()
			if err == nil {
				manager.UseProjection(projection, initializer)
				logger.Info("likes_ready", "epoch", projection.Epoch())
				err = consumeWhileHealthy(ctx, consumer, initializer, projection, producer, manager)
				consumer.Close()
			}
		}
		if ctx.Err() != nil {
			return
		}
		state := "RECOVERING"
		if errors.Is(err, likes.ErrRecoveryBlocked) {
			state = "BLOCKED"
		}
		manager.Pause(state)
		logger.Error("likes_paused", "state", state, "error", err)
		// 非法事件不能靠反复重开消费组跳过，修复后重新启动 API 再检查。
		if errors.Is(err, likes.ErrRecoveryBlocked) {
			<-ctx.Done()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// consumeWhileHealthy 也检测无新事件时的 Redis 重启；计数 Lua 会原子拒绝旧实例编号。
func consumeWhileHealthy(ctx context.Context, consumer *likes.ConsumerGroup, initializer *likes.ProjectionInitializer, projection *likes.ProjectionRepository, producer *likes.Producer, manager *realtime.Manager) error {
	consuming, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- consumer.Run(consuming, func(recordCtx context.Context, record likes.ConsumedLike) error {
			return processLike(recordCtx, record, initializer, projection, manager)
		})
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			manager.Pause("RECOVERING")
			cancel()
			<-result
			return ctx.Err()
		case <-ticker.C:
			check, finish := context.WithTimeout(ctx, time.Second)
			err := projection.CheckInstance(check)
			if err == nil {
				err = producer.Ping(check)
			}
			finish()
			if err == nil && !manager.Available() {
				err = likes.ErrProjectionRecovery
			}
			if err != nil {
				manager.Pause("RECOVERING")
				cancel()
				<-result
				return err
			}
		}
	}
}

// processLike 按记录完成投影初始化、原子计数和内存同步，返回成功后消费循环才提交位移。
func processLike(ctx context.Context, record likes.ConsumedLike, initializer *likes.ProjectionInitializer, projection *likes.ProjectionRepository, manager *realtime.Manager) error {
	if err := initializer.Ensure(ctx, record.Event); err != nil {
		return err
	}
	result, err := projection.ApplyLike(ctx, record)
	// 此刻固定 Redis 结果与结束时间，将指标登记留到内存同步之后。
	defer manager.ObserveProjection(record, result.Duplicate, err, time.Now())
	if err != nil {
		return err
	}
	return manager.QueueLikeUpdate(ctx, record.Event, result)
}

package danmaku

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RecentRecovery 从已有持久化数据重建缓存。并发请求通过 Redis 锁合并。
type RecentRecovery struct {
	store      *RecentStore
	repository *Repository
	brokers    []string
	slots      chan struct{}
	ctx        context.Context
	mu         sync.Mutex
	stopped    bool
	running    map[string]bool
	wg         sync.WaitGroup
}

func NewRecentRecovery(ctx context.Context, store *RecentStore, repository *Repository, brokers []string) *RecentRecovery {
	return &RecentRecovery{ctx: ctx, store: store, repository: repository, brokers: brokers, slots: make(chan struct{}, 4), running: make(map[string]bool)}
}

// Trigger 合并本进程内重复请求，后台任务受 API 生命周期约束。
func (recovery *RecentRecovery) Trigger(room, session string) {
	key := recentKey(room, session)
	recovery.mu.Lock()
	if recovery.stopped || recovery.running[key] {
		recovery.mu.Unlock()
		return
	}
	select {
	case recovery.slots <- struct{}{}:
	default:
		recovery.mu.Unlock()
		return
	}
	recovery.running[key] = true
	recovery.wg.Add(1)
	recovery.mu.Unlock()
	go func() {
		defer recovery.wg.Done()
		defer func() { recovery.mu.Lock(); delete(recovery.running, key); <-recovery.slots; recovery.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(recovery.ctx, 45*time.Second)
		defer cancel()
		if err := recovery.Recover(ctx, room, session); err != nil && !errors.Is(err, ErrCacheWarming) && recovery.ctx.Err() == nil {
			slog.Warn("danmaku_recent_recovery_failed", "error", err)
		}
	}()
}

func (recovery *RecentRecovery) Close() {
	recovery.mu.Lock()
	recovery.stopped = true
	recovery.mu.Unlock()
	recovery.wg.Wait()
}

//go:embed lua/recent_finish.lua
var finishRecentRecoveryLua string

var finishRecentRecovery = redis.NewScript(finishRecentRecoveryLua)

//go:embed lua/recent_release.lua
var releaseRecentRecoveryLua string

var releaseRecentRecovery = redis.NewScript(releaseRecentRecoveryLua)

// Recover 在记录 Kafka 末端后等待 MySQL 事务检查点，恢复期间阻止该场次缓存消费者写入。
func (recovery *RecentRecovery) Recover(ctx context.Context, room, session string) error {
	key := recentKey(room, session)
	token := uuid.NewString()
	lock := key + ":lock"
	temp := key + ":build:" + token
	acquired, err := recovery.store.client.SetNX(ctx, lock, token, 60*time.Second).Result()
	if err != nil {
		return err
	}
	if !acquired {
		return ErrCacheWarming
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = releaseRecentRecovery.Run(cleanup, recovery.store.client, []string{lock, temp}, token).Err()
	}()
	roomID, err := strconv.ParseUint(room, 10, 64)
	if err != nil {
		return err
	}
	sessionID, err := strconv.ParseUint(session, 10, 64)
	if err != nil {
		return err
	}
	valid, err := recovery.repository.SessionBelongs(ctx, roomID, sessionID)
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("直播场次不存在")
	}
	consumer, err := NewConsumer(ctx, recovery.brokers, RecentCacheGroup)
	if err != nil {
		return err
	}
	defer consumer.Close()
	targets, err := consumer.logOffsets(ctx, -1)
	if err != nil {
		return err
	}
	for {
		ready, err := recovery.repository.RecentCheckpointReady(ctx, consumer.TopicID(), targets)
		if err != nil {
			return err
		}
		if ready {
			break
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	now, err := recovery.store.client.Time(ctx).Result()
	if err != nil {
		return err
	}
	var lastID uint64
	for {
		rows, err := recovery.repository.RecentBatch(ctx, roomID, sessionID, lastID, now.Add(-5*time.Minute))
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		values := make([]redis.Z, 0, len(rows))
		for _, row := range rows {
			raw, err := json.Marshal(row.Event())
			if err != nil {
				return err
			}
			score := row.CreatedAt.UnixMilli()
			if score > now.UnixMilli() {
				score = now.UnixMilli()
			}
			values = append(values, redis.Z{Score: float64(score), Member: string(raw)})
			lastID = row.ID
		}
		_, err = recovery.store.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAddNX(ctx, temp, values...)
			pipe.Expire(ctx, temp, time.Minute)
			return nil
		})
		if err != nil {
			return err
		}
	}
	result, err := finishRecentRecovery.Run(ctx, recovery.store.client, []string{lock, key, temp, key + ":ready"}, token).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return fmt.Errorf("%w: 恢复锁已过期", ErrCacheWarming)
	}
	return nil
}

// RecentBatch 限制每次读取的记录数，按主键推进恢复游标。
func (repository *Repository) RecentBatch(ctx context.Context, room, session, after uint64, cutoff time.Time) ([]Message, error) {
	var rows []Message
	err := repository.db.WithContext(ctx).Where("room_id = ? AND session_id = ? AND id > ? AND created_at > ?", room, session, after, cutoff).Order("id ASC").Limit(500).Find(&rows).Error
	return rows, err
}

// RecentCheckpointReady 禁止将隔离未解决事件视为已成功恢复。
func (repository *Repository) RecentCheckpointReady(ctx context.Context, topic string, targets map[int32]int64) (bool, error) {
	var topics []string
	if err := repository.db.WithContext(ctx).Table("worker_checkpoints").Distinct("topic_id").Where("consumer_group = ?", PersistenceGroup).Pluck("topic_id", &topics).Error; err != nil {
		return false, err
	}
	for _, savedTopic := range topics {
		if savedTopic != topic {
			return false, ErrBlocked
		}
	}
	for partition, target := range targets {
		if target <= 0 {
			continue
		}
		var rows []struct{ NextOffset int64 }
		err := repository.db.WithContext(ctx).Table("worker_checkpoints").Select("next_offset").Where("consumer_group = ? AND topic_id = ? AND partition_id = ?", PersistenceGroup, topic, partition).Limit(1).Find(&rows).Error
		if err != nil {
			return false, err
		}
		if len(rows) == 0 || rows[0].NextOffset < target {
			return false, nil
		}
		var count int64
		err = repository.db.WithContext(ctx).Table("failed_events").Where("consumer_group = ? AND topic_id = ? AND partition_id = ? AND record_offset < ? AND status = ?", PersistenceGroup, topic, partition, target, "OPEN").Count(&count).Error
		if err != nil {
			return false, err
		}
		if count > 0 {
			return false, errors.New("存在尚未修复的弹幕隔离事件")
		}
	}
	return true, nil
}

package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ValidateProjectionStartup 核对 Topic、Redis 实例代次和已提交位移，发现回退时停止消费。
func (initializer *ProjectionInitializer) ValidateProjectionStartup(ctx context.Context, topicID string, committed map[int32]int64) error {
	return initializer.validateProjectionStartup(ctx, topicID, committed, false)
}

func (initializer *ProjectionInitializer) validateProjectionStartup(ctx context.Context, topicID string, committed map[int32]int64, backfill bool) error {
	prefix := "ls:v1:projection:" + initializer.projection.epoch + ":"
	var existing int64
	if err := initializer.db.WithContext(ctx).Table("projection_sessions").Where("epoch = ?", initializer.projection.epoch).Count(&existing).Error; err != nil {
		return err
	}
	info, err := initializer.projection.client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	runID := ""
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			runID = strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
			break
		}
	}
	if runID == "" {
		return errors.New("无法识别 Redis 实例运行代次")
	}
	expected := topicID + ":" + runID
	identityKey := prefix + "identity"
	identity, err := initializer.projection.client.Get(ctx, identityKey).Result()
	if errors.Is(err, redis.Nil) {
		if existing > 0 {
			return errors.New("Redis 投影身份丢失，需要恢复")
		}
		for _, offset := range committed {
			if offset > 0 {
				return errors.New("Redis 投影缺失但 Kafka 已提交位移，需要恢复")
			}
		}
		created, err := initializer.projection.client.SetNX(ctx, identityKey, expected, 0).Result()
		if err != nil {
			return err
		}
		if !created {
			identity, err = initializer.projection.client.Get(ctx, identityKey).Result()
			if err != nil {
				return err
			}
		} else {
			identity = expected
		}
	} else if err != nil {
		return err
	}
	if identity != expected {
		return errors.New("Redis 实例或 Kafka Topic 身份改变，需要恢复")
	}
	var sessions []struct {
		SessionID uint64
		RoomID    uint64
		Status    string
	}
	if err := initializer.db.WithContext(ctx).Table("projection_sessions").Select("session_id,room_id,status").Where("epoch = ?", initializer.projection.epoch).Find(&sessions).Error; err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Status != "READY" {
			return errors.New("存在未完成初始化的投影")
		}
		key := prefix + "session:" + strconv.FormatUint(session.SessionID, 10)
		values, err := initializer.projection.client.HMGet(ctx, key, "room_id", "likes", "revision", "initialized").Result()
		if err != nil {
			return err
		}
		if len(values) != 4 || values[0] != strconv.FormatUint(session.RoomID, 10) || values[1] == nil || values[2] == nil || values[3] != "1" {
			return fmt.Errorf("场次 %d 的 Redis 投影缺失或无效", session.SessionID)
		}
		if !backfill {
			if _, err := initializer.projection.ReadBoard(ctx, strconv.FormatUint(session.RoomID, 10), strconv.FormatUint(session.SessionID, 10), "", ""); err != nil {
				return fmt.Errorf("场次 %d 的点赞榜未就绪，请核对后执行 backfill-like-board: %w", session.SessionID, err)
			}
		}
	}
	for partition, offset := range committed {
		raw, err := initializer.projection.client.Get(ctx, prefix+"partition:"+strconv.FormatInt(int64(partition), 10)).Result()
		if errors.Is(err, redis.Nil) {
			if offset > 0 {
				return fmt.Errorf("分区 %d 的 Redis 位移丢失", partition)
			}
			continue
		}
		if err != nil {
			return err
		}
		current, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || current < 0 {
			return errors.New("Redis 位移无效")
		}
		if offset < 0 {
			return fmt.Errorf("分区 %d 已有投影但 Kafka 位移丢失", partition)
		}
		if backfill && current > offset {
			return fmt.Errorf("分区 %d 的 Redis 位移超出补建屏障", partition)
		}
		if current < offset {
			return fmt.Errorf("分区 %d 的 Redis 位移落后于 Kafka", partition)
		}
	}
	return nil
}

// ValidatePersistenceStartup 核对数据库检查点与 Kafka 位移，防止以错误 Topic 继续消费。
func (repository *PersistenceRepository) ValidatePersistenceStartup(ctx context.Context, topicID string, committed map[int32]int64) error {
	return repository.validateCheckpoints(ctx, topicID, committed, false)
}

func (repository *PersistenceRepository) validateCheckpoints(ctx context.Context, topicID string, committed map[int32]int64, atLogEnd bool) error {
	var rows []struct {
		TopicID     string
		PartitionID int32
		NextOffset  int64
	}
	if err := repository.db.WithContext(ctx).Table("worker_checkpoints").Select("topic_id,partition_id,next_offset").Where("consumer_group = ?", "live-persistence").Find(&rows).Error; err != nil {
		return err
	}
	checkpoints := make(map[int32]int64, len(rows))
	for _, row := range rows {
		if row.TopicID != topicID {
			return errors.New("持久化 Topic 身份改变，需要恢复")
		}
		checkpoints[row.PartitionID] = row.NextOffset
	}
	for partition, offset := range committed {
		checkpoint, exists := checkpoints[partition]
		if !exists {
			if offset > 0 {
				return fmt.Errorf("分区 %d 数据库检查点丢失", partition)
			}
			continue
		}
		if offset < 0 {
			return fmt.Errorf("分区 %d Kafka 位移丢失", partition)
		}
		if atLogEnd && checkpoint > offset {
			return fmt.Errorf("分区 %d 数据库检查点超出补建屏障", partition)
		}
		if checkpoint < offset {
			return fmt.Errorf("分区 %d 数据库检查点落后于 Kafka", partition)
		}
	}
	return nil
}

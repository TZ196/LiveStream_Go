package likes

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"strconv"
	"time"
)

// PersistenceRepository 保存点赞事件和分区检查点。
type PersistenceRepository struct{ db *gorm.DB }

// NewPersistenceRepository 使用现有 GORM 连接池创建持久化仓库。
func NewPersistenceRepository(db *gorm.DB) *PersistenceRepository {
	return &PersistenceRepository{db: db}
}

// PersistLike 在同一事务写入去重记录、点赞事件和下一条位移。
// 重复事件必须与先前内容一致；发生冲突或任何数据库错误时回滚且不能提交 Kafka 位移。
func (repository *PersistenceRepository) PersistLike(ctx context.Context, record ConsumedLike) error {
	if record.TopicID == "" {
		return errors.New("缺少 Kafka topic 身份")
	}
	event := record.Event
	return repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sql := `INSERT INTO consumer_inbox (event_id, request_id, request_hash, event_type, user_id, room_id, session_id, event_time, event_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		result := tx.Exec(sql, event.EventID, event.RequestID, event.RequestHash, event.EventType, event.UserID, event.RoomID, event.SessionID, time.UnixMilli(event.Timestamp).UTC(), string(record.RawJSON))
		if result.Error != nil {
			var mysqlErr *mysql.MySQLError
			if !errors.As(result.Error, &mysqlErr) || mysqlErr.Number != 1062 {
				return result.Error
			}
			var existing struct {
				EventID     string
				RequestID   string
				RequestHash string
				EventType   string
				UserID      uint64
				RoomID      uint64
				SessionID   uint64
			}
			err := tx.Table("consumer_inbox").Where("event_id = ?", event.EventID).Take(&existing).Error
			if err != nil {
				return fmt.Errorf("读取重复事件失败: %w", err)
			}
			if existing.EventID != event.EventID || existing.RequestID != event.RequestID || existing.RequestHash != event.RequestHash || existing.EventType != event.EventType || strconv.FormatUint(existing.UserID, 10) != event.UserID || strconv.FormatUint(existing.RoomID, 10) != event.RoomID || strconv.FormatUint(existing.SessionID, 10) != event.SessionID {
				return errors.New("相同事件 ID 的内容发生冲突")
			}
		} else {
			sql = `INSERT INTO interaction_events (event_id, room_id, session_id, user_id, event_type, created_at, click_count) VALUES (?, ?, ?, ?, ?, ?, ?)`
			if err := tx.Exec(sql, event.EventID, event.RoomID, event.SessionID, event.UserID, event.EventType, time.UnixMilli(event.Timestamp).UTC(), event.ClickCount()).Error; err != nil {
				return err
			}
		}
		sql = `INSERT INTO worker_checkpoints (consumer_group, topic_id, partition_id, next_offset) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE next_offset = GREATEST(next_offset, VALUES(next_offset))`
		return tx.Exec(sql, "live-persistence", record.TopicID, record.Partition, record.NextOffset).Error
	})
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

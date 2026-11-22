package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/gorm"
)

// PersistenceRepository 保存点赞事件和分区检查点。
type PersistenceRepository struct{ db *gorm.DB }

// NewPersistenceRepository 使用现有 GORM 连接池创建持久化仓库。
func NewPersistenceRepository(db *gorm.DB) *PersistenceRepository {
	return &PersistenceRepository{db: db}
}

// DecodeLikeRecord 校验来自 Kafka 的外壳、事件身份、指纹和分区键。
// 消费积压时不使用当前时间窗口，避免拒绝已经接受的旧事件。
func DecodeLikeRecord(record *kgo.Record) (LikeEvent, error) {
	if record.Topic != "live.events" || len(record.Value) > 64*1024 {
		return LikeEvent{}, errors.New("事件 topic 或大小无效")
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	var event LikeEvent
	if err := decoder.Decode(&event); err != nil {
		return LikeEvent{}, fmt.Errorf("事件格式无效: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return LikeEvent{}, errors.New("事件包含多余内容")
	}
	expected, err := NewLikeEvent(event.UserID, event.RoomID, event.SessionID, event.RequestID, time.UnixMilli(event.Timestamp))
	if err != nil {
		return LikeEvent{}, err
	}
	if event != expected {
		return LikeEvent{}, errors.New("事件身份、内容或指纹不匹配")
	}
	key, _, err := expected.KafkaRecord()
	if err != nil {
		return LikeEvent{}, err
	}
	if !bytes.Equal(key, record.Key) {
		return LikeEvent{}, errors.New("事件分区键不匹配")
	}
	return event, nil
}

// PersistLike 在同一事务写入去重记录、点赞事件和下一条位移。
// 重复事件必须与先前内容一致；发生冲突或任何数据库错误时回滚且不能提交 Kafka 位移。
func (repository *PersistenceRepository) PersistLike(ctx context.Context, record *kgo.Record, topicID string) error {
	if topicID == "" {
		return errors.New("缺少 Kafka topic 身份")
	}
	event, err := DecodeLikeRecord(record)
	if err != nil {
		return err
	}
	return repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sql := `INSERT INTO consumer_inbox (event_id, request_id, request_hash, event_type, user_id, room_id, session_id, event_time, event_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		result := tx.Exec(sql, event.EventID, event.RequestID, event.RequestHash, event.EventType, event.UserID, event.RoomID, event.SessionID, time.UnixMilli(event.Timestamp).UTC(), string(record.Value))
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
			sql = `INSERT INTO interaction_events (event_id, room_id, session_id, user_id, event_type, created_at) VALUES (?, ?, ?, ?, ?, ?)`
			if err := tx.Exec(sql, event.EventID, event.RoomID, event.SessionID, event.UserID, event.EventType, time.UnixMilli(event.Timestamp).UTC()).Error; err != nil {
				return err
			}
		}
		sql = `INSERT INTO worker_checkpoints (consumer_group, topic_id, partition_id, next_offset) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE next_offset = GREATEST(next_offset, VALUES(next_offset))`
		return tx.Exec(sql, "live-persistence", topicID, record.Partition, record.Offset+1).Error
	})
}

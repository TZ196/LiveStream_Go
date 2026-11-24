package likes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

type projectionState struct {
	ID      uint8 `gorm:"primaryKey"`
	Epoch   string
	TopicID string
}

func (projectionState) TableName() string { return "projection_state" }

// ProjectionEpoch 首次兼容现有配置，以后以数据库中成功启用的代次为准。
func (repository *PersistenceRepository) ProjectionEpoch(ctx context.Context, fallback, topicID string) (string, error) {
	state := projectionState{ID: 1, Epoch: fallback, TopicID: topicID}
	if err := repository.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error; err != nil {
		return "", err
	}
	if err := repository.db.WithContext(ctx).First(&state, 1).Error; err != nil {
		return "", err
	}
	id, err := uuid.Parse(state.Epoch)
	if err != nil || id.String() != state.Epoch || state.TopicID != topicID {
		return "", fmt.Errorf("%w: 有效投影代次或 Topic 身份不匹配", ErrRecoveryBlocked)
	}
	return state.Epoch, nil
}

func (repository *PersistenceRepository) saveProjectionEpoch(ctx context.Context, previous, next, topicID string) error {
	result := repository.db.WithContext(ctx).Model(&projectionState{}).
		Where("id = 1 AND epoch = ? AND topic_id = ?", previous, topicID).Update("epoch", next)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: 有效投影代次被其他进程改变", ErrRecoveryBlocked)
	}
	return nil
}

func (repository *PersistenceRepository) checkpoints(ctx context.Context, topicID string) (map[int32]int64, error) {
	var rows []struct {
		TopicID     string
		PartitionID int32
		NextOffset  int64
	}
	err := repository.db.WithContext(ctx).Table("worker_checkpoints").Where("consumer_group = ?", "live-persistence").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	answer := make(map[int32]int64)
	for _, row := range rows {
		if row.TopicID != topicID || row.PartitionID < 0 || row.PartitionID >= 16 || row.NextOffset < 0 {
			return nil, fmt.Errorf("%w: MySQL 检查点身份或位置无效", ErrRecoveryBlocked)
		}
		answer[row.PartitionID] = row.NextOffset
	}
	return answer, nil
}

// validateRecoveryHistory 同时检查两个方向，不能只比较总行数或认为有表就代表完整。
func (repository *PersistenceRepository) validateRecoveryHistory(ctx context.Context) error {
	var invalid bool
	query := `SELECT EXISTS (
        SELECT 1 FROM consumer_inbox i LEFT JOIN interaction_events e ON e.event_id = i.event_id
        WHERE e.event_id IS NULL OR i.event_type <> 'LIKE_CLICKED' OR e.event_type <> i.event_type
           OR e.user_id <> i.user_id OR e.room_id <> i.room_id OR e.session_id <> i.session_id
    ) OR EXISTS (
        SELECT 1 FROM interaction_events e LEFT JOIN consumer_inbox i ON i.event_id = e.event_id
        WHERE i.event_id IS NULL
    ) OR EXISTS (
        SELECT event_id FROM interaction_events GROUP BY event_id HAVING COUNT(*) > 1
    )`
	if err := repository.db.WithContext(ctx).Raw(query).Scan(&invalid).Error; err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("%w: 点赞明细与去重记录不完整或不一致", ErrRecoveryBlocked)
	}
	return nil
}

type recoveryEvent struct {
	EventID     string
	RequestID   string
	RequestHash string
	UserID      uint64
	RoomID      uint64
	SessionID   uint64
	EventTime   time.Time
	EventJSON   string
	ClickCount  int64
}

// recoveryEvents 每页最多读取 256 条；恢复时再次验证冻结内容和批次数量。
func (repository *PersistenceRepository) recoveryEvents(ctx context.Context, after string) ([]recoveryEvent, error) {
	var rows []recoveryEvent
	err := repository.db.WithContext(ctx).Table("consumer_inbox AS i").
		Joins("JOIN interaction_events AS e ON e.event_id = i.event_id").
		Select("i.event_id,i.request_id,i.request_hash,i.user_id,i.room_id,i.session_id,i.event_time,i.event_json,e.click_count").
		Where("i.event_id > ?", after).Order("i.event_id").Limit(256).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		var event LikeEvent
		if len(row.EventJSON) > 64*1024 || json.NewDecoder(bytes.NewBufferString(row.EventJSON)).Decode(&event) != nil {
			return nil, fmt.Errorf("%w: 已保存的事件格式无效", ErrRecoveryBlocked)
		}
		raw := &kgo.Record{Topic: "live.events", Key: []byte(event.RoomID + ":" + event.EventID), Value: []byte(row.EventJSON)}
		decoded, err := DecodeLikeRecord(raw, "recovery-validation")
		if err != nil || event.EventID != row.EventID || event.RequestID != row.RequestID || event.RequestHash != row.RequestHash ||
			event.UserID != strconv.FormatUint(row.UserID, 10) || event.RoomID != strconv.FormatUint(row.RoomID, 10) ||
			event.SessionID != strconv.FormatUint(row.SessionID, 10) || !time.UnixMilli(event.Timestamp).Equal(row.EventTime) || decoded.Event.ClickCount() != row.ClickCount {
			return nil, fmt.Errorf("%w: 已保存的事件身份、指纹或点击数不一致", ErrRecoveryBlocked)
		}
	}
	return rows, nil
}

// recoverySessions 从场次表读取，零赞和已结束场次也参加恢复。
func (repository *PersistenceRepository) recoverySessions(ctx context.Context, after uint64) ([]projectionSession, error) {
	var sessions []projectionSession
	err := repository.db.WithContext(ctx).Table("live_sessions").Select("id AS session_id,room_id").
		Where("id > ?", after).Order("id").Limit(256).Scan(&sessions).Error
	return sessions, err
}

type recoveryCount struct {
	UserID uint64
	Clicks int64
}

func (repository *PersistenceRepository) recoveryCounts(ctx context.Context, sessionID, after uint64) ([]recoveryCount, error) {
	var counts []recoveryCount
	err := repository.db.WithContext(ctx).Table("interaction_events").Select("user_id,SUM(click_count) AS clicks").
		Where("session_id = ? AND user_id > ?", sessionID, after).Group("user_id").Order("user_id").Limit(256).Scan(&counts).Error
	return counts, err
}

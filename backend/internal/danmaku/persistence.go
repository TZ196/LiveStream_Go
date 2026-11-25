package danmaku

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	mysqlerror "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type Repository struct{ db *gorm.DB }

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

type Message struct {
	ID                uint64    `json:"id,string"`
	EventID           string    `json:"event_id"`
	RequestID         string    `json:"request_id"`
	RequestHash       string    `json:"-"`
	UserID            uint64    `json:"user_id,string"`
	Username          string    `json:"username"`
	RoomID            uint64    `json:"room_id,string"`
	SessionID         uint64    `json:"session_id,string"`
	Content           string    `json:"content"`
	ModerationVersion string    `json:"-"`
	CreatedAt         time.Time `json:"created_at"`
	PersistedAt       time.Time `json:"-" gorm:"type:datetime(3);default:CURRENT_TIMESTAMP(3)"`
}

func (Message) TableName() string { return "danmaku_messages" }

func (message Message) Event() Event {
	return Event{SchemaVersion: 1, EventID: message.EventID, EventType: "DANMAKU_SENT", RequestID: message.RequestID, RequestHash: message.RequestHash,
		UserID: strconv.FormatUint(message.UserID, 10), RoomID: strconv.FormatUint(message.RoomID, 10), SessionID: strconv.FormatUint(message.SessionID, 10),
		Payload: Payload{Username: message.Username, Content: message.Content}, ModerationVersion: message.ModerationVersion, Timestamp: message.CreatedAt.UnixMilli()}
}

func sameEvent(first, second Event) bool { return first == second }

func (repository *Repository) FindRequest(ctx context.Context, user, request string) (Event, bool, error) {
	var message Message
	err := repository.db.WithContext(ctx).Where("user_id = ? AND request_id = ?", user, request).Take(&message).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Event{}, false, nil
	}
	if err != nil {
		return Event{}, false, err
	}
	return message.Event(), true, nil
}

// Persist 在同一事务中保存业务事件和本组位置。重复写必须核对全部冻结字段。
func (repository *Repository) Persist(ctx context.Context, record Record) error {
	if record.TopicID == "" || record.NextOffset < 1 {
		return errors.New("缺少弹幕 Topic 或位置")
	}
	event := record.Event
	createdAt := time.UnixMilli(event.Timestamp).UTC()
	// 异常时间可能在 MySQL 驱动发送前就被拒绝，应按坏数据隔离，不能误当成断连。
	if createdAt.Year() < 1000 || createdAt.Year() > 9999 {
		return fmt.Errorf("%w: 事件时间超出 MySQL 日期范围", ErrInvalidEvent)
	}
	userID, _ := strconv.ParseUint(event.UserID, 10, 64)
	roomID, _ := strconv.ParseUint(event.RoomID, 10, 64)
	sessionID, _ := strconv.ParseUint(event.SessionID, 10, 64)
	return repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := Message{
			EventID:           event.EventID,
			RequestID:         event.RequestID,
			RequestHash:       event.RequestHash,
			UserID:            userID,
			Username:          event.Payload.Username,
			RoomID:            roomID,
			SessionID:         sessionID,
			Content:           event.Payload.Content,
			ModerationVersion: event.ModerationVersion,
			CreatedAt:         createdAt,
		}
		err := tx.Create(&row).Error
		if err != nil {
			var duplicate *mysqlerror.MySQLError
			if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
				// 只把明确的数据约束错误判为永久异常；断连、死锁和表结构故障继续重试。
				if errors.As(err, &duplicate) {
					switch duplicate.Number {
					case 1264, 1292, 1366, 1406, 1452, 3819:
						return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
					}
				}
				return err
			}
			var old Message
			if err := tx.Where("user_id = ? AND request_id = ?", userID, event.RequestID).Take(&old).Error; err != nil {
				return err
			}
			if !sameEvent(old.Event(), event) {
				return ErrConflict
			}
		}
		return advanceCheckpoint(tx, record.TopicID, record.Partition, record.NextOffset)
	})
}

// advanceCheckpoint 必须与本条业务写入或异常隔离共用事务。
func advanceCheckpoint(tx *gorm.DB, topicID string, partition int32, nextOffset int64) error {
	return tx.Exec(`INSERT INTO worker_checkpoints (consumer_group,topic_id,partition_id,next_offset) VALUES (?,?,?,?)
        ON DUPLICATE KEY UPDATE next_offset=GREATEST(next_offset,VALUES(next_offset))`, PersistenceGroup, topicID, partition, nextOffset).Error
}

// ValidateCheckpoint 接受事务领先 Kafka 的正常重复窗口，但拒绝 Kafka 越过数据库或换 Topic。
func (repository *Repository) ValidateCheckpoint(ctx context.Context, topicID string, committed map[int32]int64) error {
	var rows []struct {
		TopicID     string
		PartitionID int32
		NextOffset  int64
	}
	if err := repository.db.WithContext(ctx).Table("worker_checkpoints").Where("consumer_group = ?", PersistenceGroup).Find(&rows).Error; err != nil {
		return err
	}
	saved := make(map[int32]int64)
	for _, row := range rows {
		if row.TopicID != topicID || row.NextOffset < 0 {
			return ErrBlocked
		}
		saved[row.PartitionID] = row.NextOffset
	}
	var count int64
	if err := repository.db.WithContext(ctx).Model(&Message{}).Count(&count).Error; err != nil {
		return err
	}
	for partition, offset := range committed {
		if offset < 0 {
			if saved[partition] > 0 || (count > 0 && len(rows) == 0) {
				return ErrBlocked
			}
			continue
		}
		if saved[partition] < offset {
			return ErrBlocked
		}
	}
	return nil
}

type cursor struct {
	Room    uint64 `json:"room"`
	Session uint64 `json:"session"`
	Before  uint64 `json:"before"`
}
type History struct {
	Items       []Message `json:"items"`
	NextCursor  *string   `json:"next_cursor"`
	Consistency string    `json:"consistency"`
}

// History 使用绑定筛选条件的不透明游标；所有条件均由 GORM 参数化。
func (repository *Repository) History(ctx context.Context, room, session uint64, limit int, encoded string) (History, error) {
	if limit < 1 || limit > 100 || len(encoded) > 512 {
		return History{}, failure("INVALID_ARGUMENT", "历史分页参数无效", false)
	}
	query := repository.db.WithContext(ctx).Where("room_id = ?", room)
	if session != 0 {
		query = query.Where("session_id = ?", session)
	}
	if encoded != "" {
		raw, err := base64.RawURLEncoding.DecodeString(encoded)
		var position cursor
		if err != nil || json.Unmarshal(raw, &position) != nil || position.Room != room || position.Session != session || position.Before == 0 {
			return History{}, failure("INVALID_CURSOR", "历史游标与房间或场次不匹配", false)
		}
		query = query.Where("id < ?", position.Before)
	}
	items := make([]Message, 0, limit+1)
	if err := query.Order("id DESC").Limit(limit + 1).Find(&items).Error; err != nil {
		return History{}, err
	}
	result := History{Items: items, Consistency: "eventual"}
	if len(items) > limit {
		result.Items = items[:limit]
		raw, _ := json.Marshal(cursor{Room: room, Session: session, Before: result.Items[limit-1].ID})
		next := base64.RawURLEncoding.EncodeToString(raw)
		result.NextCursor = &next
	}
	return result, nil
}

func (repository *Repository) HasHistory(ctx context.Context) (bool, error) {
	var count int64
	err := repository.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM danmaku_messages LIMIT 1)").Scan(&count).Error
	return count > 0, err
}

func (repository *Repository) SessionBelongs(ctx context.Context, room, session uint64) (bool, error) {
	var exists bool
	err := repository.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM live_sessions WHERE room_id=? AND id=?)", room, session).Scan(&exists).Error
	return exists, err
}

func (message Message) MarshalJSON() ([]byte, error) {
	type alias Message
	return json.Marshal(struct {
		alias
		Created string `json:"created_at"`
	}{alias: alias(message), Created: message.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z")})
}

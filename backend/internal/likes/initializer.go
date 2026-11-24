package likes

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
	"strings"
)

// ProjectionInitializer 记录实时投影是否初始化过，避免丢失后把旧计数当成零。
type ProjectionInitializer struct {
	db         *gorm.DB
	projection *ProjectionRepository
}

type projectionSession struct {
	Epoch     string `gorm:"primaryKey"`
	SessionID uint64 `gorm:"primaryKey"`
	RoomID    uint64
	Status    string
}

// TableName 指定投影初始化状态表。
func (projectionSession) TableName() string { return "projection_sessions" }

// NewProjectionInitializer 组装 MySQL 持久标记和 Redis 投影操作。
func NewProjectionInitializer(db *gorm.DB, projection *ProjectionRepository) *ProjectionInitializer {
	return &ProjectionInitializer{db: db, projection: projection}
}

// Ensure 在消费前初始化新场次；已初始化的场次丢失投影时拒绝重建。
// 数据库行锁串行化首次初始化；初始化成功提交后才允许任何事件增加计数。
func (initializer *ProjectionInitializer) Ensure(ctx context.Context, event LikeEvent) error {
	roomID, err := strconv.ParseUint(event.RoomID, 10, 64)
	if err != nil {
		return err
	}
	sessionID, err := strconv.ParseUint(event.SessionID, 10, 64)
	if err != nil {
		return err
	}
	return initializer.EnsureSession(ctx, roomID, sessionID)
}

// EnsureSession 由已校验场次的读入口和消费者共享，首赞前也能返回真实的空榜。
func (initializer *ProjectionInitializer) EnsureSession(ctx context.Context, roomID, sessionID uint64) error {
	return initializer.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := projectionSession{Epoch: initializer.projection.epoch, SessionID: sessionID, RoomID: roomID, Status: "INITIALIZING"}
		insert := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
		if insert.Error != nil {
			return insert.Error
		}
		var stored projectionSession
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("epoch = ? AND session_id = ?", row.Epoch, sessionID).Take(&stored).Error; err != nil {
			return err
		}
		if stored.RoomID != roomID {
			return errors.New("场次与房间不匹配")
		}
		key := "ls:v1:projection:" + row.Epoch + ":session:" + strconv.FormatUint(sessionID, 10)
		switch stored.Status {
		case "INITIALIZING":
			if err := initializer.projection.InitializeNewSession(ctx, strconv.FormatUint(roomID, 10), strconv.FormatUint(sessionID, 10)); err != nil {
				return err
			}
			return tx.Model(&projectionSession{}).Where("epoch = ? AND session_id = ?", row.Epoch, sessionID).Update("status", "READY").Error
		case "READY":
			exists, err := initializer.projection.client.Exists(ctx, key).Result()
			if err != nil {
				return err
			}
			if exists != 1 {
				return errors.New("已初始化的 Redis 场次投影丢失，需要恢复")
			}
			return nil
		default:
			return fmt.Errorf("未知投影状态 %q", stored.Status)
		}
	})
}

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

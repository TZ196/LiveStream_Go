package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

// ReadSession 读取已初始化场次计数；缺失或 Redis 不可用不返回伪造的零。
func (repository *ProjectionRepository) ReadSession(ctx context.Context, roomID, sessionID string) (ProjectedLike, error) {
	key := "ls:v1:projection:" + repository.epoch + ":session:" + sessionID
	values, err := repository.client.HMGet(ctx, key, "room_id", "likes", "revision", "initialized").Result()
	if err != nil {
		return ProjectedLike{}, err
	}
	if len(values) != 4 || values[0] != roomID || values[3] != "1" || values[1] == nil || values[2] == nil {
		return ProjectedLike{}, redis.Nil
	}
	count, ok := values[1].(string)
	if !ok {
		return ProjectedLike{}, errors.New("点赞数类型无效")
	}
	revision, ok := values[2].(string)
	if !ok {
		return ProjectedLike{}, errors.New("投影版本类型无效")
	}
	return ProjectedLike{Count: count, Revision: revision}, nil
}

package likes

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"sort"
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
				return fmt.Errorf("%w: 已初始化的 Redis 场次投影丢失", ErrProjectionRecovery)
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
	runID, err := redisRunID(ctx, initializer.projection.client)
	if err != nil {
		return err
	}
	expected := topicID + ":" + runID
	identityKey := prefix + "identity"
	identity, err := initializer.projection.client.Get(ctx, identityKey).Result()
	if errors.Is(err, redis.Nil) {
		if existing > 0 {
			return fmt.Errorf("%w: Redis 投影身份丢失", ErrProjectionRecovery)
		}
		for _, offset := range committed {
			if offset > 0 {
				return fmt.Errorf("%w: Redis 投影缺失但 Kafka 已提交位移", ErrProjectionRecovery)
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
	// run_id 在每次 Redis 重启后都会变化。只将 Topic 改变判为身份冲突，
	// 新运行编号由 Check 在完整检查（含领先区间去重依据）通过后登记。
	if !strings.HasPrefix(identity, topicID+":") {
		return fmt.Errorf("%w: Kafka Topic 身份改变", ErrRecoveryBlocked)
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
			return fmt.Errorf("%w: 存在未完成初始化的投影", ErrProjectionRecovery)
		}
		key := prefix + "session:" + strconv.FormatUint(session.SessionID, 10)
		values, err := initializer.projection.client.HMGet(ctx, key, "room_id", "likes", "revision", "initialized").Result()
		if err != nil {
			return err
		}
		if len(values) != 4 || values[0] != strconv.FormatUint(session.RoomID, 10) || values[1] == nil || values[2] == nil || values[3] != "1" {
			return fmt.Errorf("%w: 场次 %d 的 Redis 投影缺失或无效", ErrProjectionRecovery, session.SessionID)
		}
		if !backfill {
			if err := initializer.validateSession(ctx, session.RoomID, session.SessionID); err != nil {
				return fmt.Errorf("%w: 场次 %d 的计数或榜单不完整: %v", ErrProjectionRecovery, session.SessionID, err)
			}
		}
	}
	for partition, offset := range committed {
		raw, err := initializer.projection.client.Get(ctx, prefix+"partition:"+strconv.FormatInt(int64(partition), 10)).Result()
		if errors.Is(err, redis.Nil) {
			if offset > 0 {
				return fmt.Errorf("%w: 分区 %d 的 Redis 位移丢失", ErrProjectionRecovery, partition)
			}
			continue
		}
		if err != nil {
			return err
		}
		current, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || current < 0 || strconv.FormatInt(current, 10) != raw {
			return fmt.Errorf("%w: Redis 位移无效", ErrProjectionRecovery)
		}
		if offset < 0 {
			return fmt.Errorf("%w: 分区 %d 已有投影但 Kafka 位移丢失", ErrProjectionRecovery, partition)
		}
		if backfill && current > offset {
			return fmt.Errorf("分区 %d 的 Redis 位移超出补建屏障", partition)
		}
		if current < offset {
			return fmt.Errorf("%w: 分区 %d 的 Redis 位移落后于 Kafka", ErrProjectionRecovery, partition)
		}
	}
	return nil
}

// validateSession 分页核对所有个人计数之和与完整前百榜，不只检查键是否存在。
func (initializer *ProjectionInitializer) validateSession(ctx context.Context, roomID, sessionID uint64) error {
	repo := initializer.projection
	roomText, sessionText := strconv.FormatUint(roomID, 10), strconv.FormatUint(sessionID, 10)
	state, err := repo.ReadSession(ctx, roomText, sessionText)
	if err != nil {
		return err
	}
	var cursor uint64
	var total int64
	top := make([]RankedUser, 0, 101)
	for {
		items, next, err := repo.client.HScan(ctx, repo.sessionKey(sessionText)+":users", cursor, "", 256).Result()
		if err != nil {
			return err
		}
		for index := 0; index < len(items); index += 2 {
			user, count := items[index], items[index+1]
			id, idErr := strconv.ParseUint(user, 10, 64)
			clicks, countErr := strconv.ParseInt(count, 10, 64)
			if idErr != nil || id == 0 || strconv.FormatUint(id, 10) != user || countErr != nil || clicks < 1 || strconv.FormatInt(clicks, 10) != count || total > math.MaxInt64-clicks {
				return errors.New("个人计数无效或汇总溢出")
			}
			total += clicks
			top = append(top, RankedUser{UserID: user, Count: count})
			sort.Slice(top, func(i, j int) bool {
				left, _ := strconv.ParseInt(top[i].Count, 10, 64)
				right, _ := strconv.ParseInt(top[j].Count, 10, 64)
				if left != right {
					return left > right
				}
				leftID, _ := strconv.ParseUint(top[i].UserID, 10, 64)
				rightID, _ := strconv.ParseUint(top[j].UserID, 10, 64)
				return leftID < rightID
			})
			if len(top) > 100 {
				top = top[:100]
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if state.Count != strconv.FormatInt(total, 10) {
		return errors.New("个人数之和与场次总数不一致")
	}
	board, err := repo.ReadBoard(ctx, roomText, sessionText, "", "")
	if err != nil {
		return err
	}
	if len(top) != len(board.Top) {
		return errors.New("榜单人数不一致")
	}
	for index := range top {
		if top[index] != board.Top[index] {
			return errors.New("榜单顺序或计数不一致")
		}
	}
	return nil
}

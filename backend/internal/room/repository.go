package room

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	Open   = "OPEN"
	Closed = "CLOSED"
)

type Room struct {
	ID        uint64 `gorm:"primaryKey"`
	OwnerID   uint64
	Title     string
	Status    string
	CreatedAt time.Time `gorm:"autoCreateTime:false"`
	ClosedAt  *time.Time
}

func (Room) TableName() string { return "live_rooms" }

var ErrNotFound = errors.New("直播间不存在")

// Session 表示一个房间的一次独立开播；迁移前旧场次的开始时间可能未知。
type Session struct {
	ID        uint64 `gorm:"primaryKey"`
	RoomID    uint64
	StartedAt *time.Time
	EndedAt   *time.Time
}

// TableName 指定场次使用的数据表。
func (Session) TableName() string { return "live_sessions" }

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

// Create 把房间和首个直播场次放在同一事务里，两者都保存成功才算建房成功。
func (r *Repository) Create(ctx context.Context, room *Room) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(room).Error; err != nil {
			return err
		}
		startedAt := room.CreatedAt
		return tx.Create(&Session{RoomID: room.ID, StartedAt: &startedAt}).Error
	})
}

func (r *Repository) FindByID(ctx context.Context, id uint64) (Room, error) {
	var room Room
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&room).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Room{}, ErrNotFound
	}
	return room, err
}

func (r *Repository) List(ctx context.Context, options ListOptions, beforeID uint64) ([]Room, error) {
	query := r.db.WithContext(ctx).Where("status = ?", options.Status)
	if options.OwnerID != 0 {
		query = query.Where("owner_id = ?", options.OwnerID)
	}
	if beforeID != 0 {
		query = query.Where("id < ?", beforeID)
	}
	// 多取一条判断是否还有下一页，避免另做全表计数。
	rooms := make([]Room, 0)
	err := query.Order("id DESC").Limit(options.Limit + 1).Find(&rooms).Error
	return rooms, err
}

func (r *Repository) SetStatus(ctx context.Context, id, ownerID uint64, status string) (Room, error) {
	var room Room
	// GORM 在事务内调用此函数，失败自动回滚；行锁使同一房间的开播与关闭依次执行。
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"})
		err := query.Where("id = ? AND owner_id = ?", id, ownerID).Take(&room).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		// 同方向重试不改变状态，也不覆盖本轮直播的关闭时间。
		if room.Status == status {
			return nil
		}
		var closedAt *time.Time
		if status == Closed {
			now := time.Now().UTC().Truncate(time.Millisecond)
			closedAt = &now
		}
		if status == Closed {
			result := tx.Model(&Session{}).Where("room_id = ? AND ended_at IS NULL", id).Update("ended_at", closedAt)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errors.New("房间活动场次状态不一致")
			}
		} else {
			startedAt := time.Now().UTC().Truncate(time.Millisecond)
			if err := tx.Create(&Session{RoomID: id, StartedAt: &startedAt}).Error; err != nil {
				return err
			}
		}
		values := map[string]any{"status": status, "closed_at": closedAt}
		if err := tx.Model(&Room{}).Where("id = ?", id).Updates(values).Error; err != nil {
			return err
		}
		// 返回本次事务完成的状态，避免提交后重查时读到另一请求刚修改的状态。
		room.Status = status
		room.ClosedAt = closedAt
		return nil
	})
	return room, err
}

// ActiveSession 查询当前房间的活动场次；调用方仍需校验房间状态。
func (r *Repository) ActiveSession(ctx context.Context, roomID uint64) (Session, error) {
	var session Session
	err := r.db.WithContext(ctx).Where("room_id = ? AND ended_at IS NULL", roomID).Take(&session).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Session{}, ErrNotFound
	}
	return session, err
}

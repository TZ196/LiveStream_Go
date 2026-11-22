package room

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
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

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, room *Room) error {
	return r.db.WithContext(ctx).Create(room).Error
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

func (r *Repository) Close(ctx context.Context, id, ownerID uint64, closedAt time.Time) (Room, error) {
	// 条件更新只允许 OPEN -> CLOSED；并发或重复关闭不会覆盖第一次的时间。
	query := r.db.WithContext(ctx).Model(&Room{})
	query = query.Where("id = ? AND owner_id = ? AND status = ?", id, ownerID, Open)
	err := query.Updates(map[string]any{"status": Closed, "closed_at": closedAt}).Error
	if err != nil {
		return Room{}, err
	}
	// V1 不允许删除房间、转移所有者或重开，因此关闭后的状态不会倒退。
	return r.FindByID(ctx, id)
}

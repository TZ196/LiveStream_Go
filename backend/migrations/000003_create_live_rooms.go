package migrations

import (
	"time"

	"gorm.io/gorm"
)

// 版本 3 只新增房间表，不修改已有用户和迁移记录。
type roomV3 struct {
	ID        uint64     `gorm:"primaryKey;autoIncrement;index:idx_rooms_status_id,priority:2;index:idx_rooms_owner_id,priority:2"`
	OwnerID   uint64     `gorm:"not null;index:idx_rooms_owner_id,priority:1"`
	Title     string     `gorm:"type:varchar(80);not null"`
	Status    string     `gorm:"type:varchar(16) CHARACTER SET ascii COLLATE ascii_bin;not null;index:idx_rooms_status_id,priority:1;check:chk_live_rooms_state,(status = 'OPEN' AND closed_at IS NULL) OR (status = 'CLOSED' AND closed_at IS NOT NULL)"`
	CreatedAt time.Time  `gorm:"type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoCreateTime:false"`
	ClosedAt  *time.Time `gorm:"type:datetime(3)"`
	Owner     userV1     `gorm:"foreignKey:OwnerID;references:ID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT"`
}

func (roomV3) TableName() string { return "live_rooms" }

func createLiveRoomsV3(db *gorm.DB) error {
	withOptions := db.Set("gorm:table_options", tableOptions)
	return withOptions.Migrator().CreateTable(&roomV3{})
}

package migrations

import (
	"time"

	"gorm.io/gorm"
)

// 这是固定的版本 1 结构，不复用以后会变化的业务模型，避免旧迁移含义改变。
type userV1 struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	Username     string    `gorm:"type:varchar(32) CHARACTER SET ascii COLLATE ascii_bin;not null;uniqueIndex:uq_users_username;check:chk_users_username,REGEXP_LIKE(username, '^[a-z][a-z0-9_]{2,31}$', 'c')"`
	PasswordHash string    `gorm:"type:varchar(255) CHARACTER SET ascii COLLATE ascii_bin;not null"`
	CreatedAt    time.Time `gorm:"type:datetime(3);not null;default:CURRENT_TIMESTAMP(3);autoCreateTime:false"`
}

func (userV1) TableName() string { return "users" }

func createUsersV1(db *gorm.DB) error {
	withOptions := db.Set("gorm:table_options", tableOptions)
	return withOptions.Migrator().CreateTable(&userV1{})
}

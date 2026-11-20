package database

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// RequiredSchemaVersion 是当前程序需要的结构版本，新增迁移时同步更新。
const RequiredSchemaVersion uint64 = 2

// CurrentVersion 读取成功记录，拒绝缺号、重复或从零开始的异常历史。
func CurrentVersion(ctx context.Context, db *gorm.DB) (uint64, error) {
	var versions []uint64
	query := db.WithContext(ctx).Table("schema_migrations").Order("version")
	err := query.Pluck("version", &versions).Error
	if err != nil {
		return 0, fmt.Errorf("读取数据库迁移版本失败，请检查连接并先执行迁移: %w", err)
	}
	for index, version := range versions {
		expected := uint64(index + 1)
		if version != expected {
			return 0, fmt.Errorf("数据库迁移记录不连续：期望 %d，实际 %d", expected, version)
		}
	}
	return uint64(len(versions)), nil
}

// CheckSchema 只读检查成功版本和所需列；超时由调用方的 context 控制。
func CheckSchema(ctx context.Context, db *gorm.DB) error {
	version, err := CurrentVersion(ctx, db)
	if err != nil {
		return err
	}
	if version != RequiredSchemaVersion {
		return fmt.Errorf("数据库迁移版本不匹配：当前 %d，需要 %d", version, RequiredSchemaVersion)
	}

	// 从共享实例创建独立查询；LIMIT 0 不读取账号记录，但能发现缺表和缺列。
	query := db.WithContext(ctx).Table("users")
	query = query.Select("id", "username", "password_hash", "created_at").Limit(0)
	rows, err := query.Rows()
	if err != nil {
		return fmt.Errorf("检查 users 表失败: %w", err)
	}
	err = rows.Close()
	if err != nil {
		return fmt.Errorf("关闭 users 表检查结果失败: %w", err)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("读取 users 表检查结果失败: %w", err)
	}
	return nil
}

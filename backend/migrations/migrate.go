package migrations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"gorm.io/gorm"
)

const tableOptions = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// 只保存成功记录，字段与原 SQL 迁移一致，不增加隐含的软删除等字段。
type schemaMigration struct {
	Version   uint64    `gorm:"primaryKey;autoIncrement:false;not null"`
	AppliedAt time.Time `gorm:"type:datetime(3);not null;default:CURRENT_TIMESTAMP(3)"`
}

func (schemaMigration) TableName() string { return "schema_migrations" }

// Run 在固定数据库连接上持有命名锁，防止两个迁移命令同时修改同一数据库。
func Run(ctx context.Context, db *gorm.DB, logger *slog.Logger) error {
	migrationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Connection 的回调期间始终使用同一连接，MySQL 命名锁才不会串到其他连接。
	return db.WithContext(migrationCtx).Connection(func(connection *gorm.DB) error {
		// 各条语句使用独立查询状态，但仍共用这条持锁连接。
		conn := connection.Session(&gorm.Session{NewDB: true})
		var acquired int
		query := conn.Raw("SELECT GET_LOCK(CONCAT('livestream:migrate:', SHA1(DATABASE())), 0)")
		err := query.Scan(&acquired).Error
		if err != nil {
			return fmt.Errorf("获取数据库迁移锁失败: %w", err)
		}
		if acquired != 1 {
			return errors.New("已有迁移命令正在操作这个数据库，请等待它结束")
		}

		migrationErr := applyPending(conn, logger)
		// 即使迁移被取消，也给释放锁一个独立的短预算，然后退出并关闭连接池。
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
		var released int
		query = conn.WithContext(releaseCtx).Raw("SELECT RELEASE_LOCK(CONCAT('livestream:migrate:', SHA1(DATABASE())))")
		err = query.Scan(&released).Error
		releaseCancel()
		if err != nil {
			return errors.Join(migrationErr, fmt.Errorf("释放数据库迁移锁失败: %w", err))
		}
		if released != 1 {
			return errors.Join(migrationErr, errors.New("数据库迁移锁未正常释放，请检查连接状态"))
		}
		return migrationErr
	})
}

func applyPending(db *gorm.DB, logger *slog.Logger) error {
	migrator := db.Migrator()
	if !migrator.HasTable(&schemaMigration{}) {
		withOptions := db.Set("gorm:table_options", tableOptions)
		err := withOptions.Migrator().CreateTable(&schemaMigration{})
		if err != nil {
			return fmt.Errorf("创建迁移记录表失败: %w", err)
		}
	}
	ctx := db.Statement.Context
	current, err := database.CurrentVersion(ctx, db)
	if err != nil {
		return err
	}
	if current > database.RequiredSchemaVersion {
		return fmt.Errorf("数据库版本 %d 高于本程序支持的 %d，拒绝降级", current, database.RequiredSchemaVersion)
	}

	for version := current + 1; version <= database.RequiredSchemaVersion; version++ {
		logger.Info("migration_starting", "version", version)
		switch version {
		case 1:
			err = createUsersV1(db)
		case 2:
			err = tightenUsernameV2(db)
		case 3:
			err = createLiveRoomsV3(db)
		case 4:
			err = createLikesV4(db)
		case 5:
			err = createLikeBatchCountV5(db)
		case 6:
			err = createProjectionStateV6(db)
		case 7:
			err = createDanmakuV7(db)
		case 8:
			err = createFailedEventsV8(db)
		default:
			err = fmt.Errorf("版本 %d 尚未定义迁移步骤", version)
		}
		if err != nil {
			return fmt.Errorf("迁移版本 %d 失败，后续版本未执行；请检查实际结构后修复: %w", version, err)
		}

		// MySQL DDL 会隐式提交。只有步骤成功后才写记录，不能承诺整轮自动回滚。
		record := schemaMigration{Version: version}
		err = db.Create(&record).Error
		if err != nil {
			return fmt.Errorf("版本 %d 的结构操作已完成，但成功记录写入失败；请核对实际结构: %w", version, err)
		}
		logger.Info("migration_applied", "version", version)
	}
	// 已是最新版本时也检查所需表，不能只凭版本号宣布数据库可用。
	return database.CheckSchema(ctx, db)
}

package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"time"
)

// Open 由 GORM 创建连接池并检查连通性；API 和迁移命令分别决定后续步骤。
func Open(ctx context.Context, cfg config.Config) (*gorm.DB, error) {
	// 使用驱动配置对象，避免手工拼接带密码的连接字符串。
	driverConfig := mysqldriver.NewConfig()
	driverConfig.Net = "tcp"
	driverConfig.Addr = cfg.MySQLAddr
	driverConfig.DBName = cfg.MySQLDatabase
	driverConfig.User = cfg.MySQLUser
	driverConfig.Passwd = cfg.MySQLPassword
	driverConfig.ParseTime = true
	driverConfig.Loc = time.UTC
	driverConfig.Collation = "utf8mb4_0900_ai_ci"
	driverConfig.Params = map[string]string{"time_zone": "'+00:00'"}
	driverConfig.Timeout = 5 * time.Second
	driverConfig.ReadTimeout = 5 * time.Second
	driverConfig.WriteTimeout = 5 * time.Second

	// 不传入已有连接池，由 GORM 的 MySQL 适配器创建；不手工拼接密码。
	dialect := mysql.New(mysql.Config{
		DSNConfig:                 driverConfig,
		SkipInitializeWithVersion: true,
	})
	ormConfig := &gorm.Config{
		// 由下面带 context 的 PingContext 检查连接，保留超时和取消能力。
		DisableAutomaticPing: true,
		// 错误交给调用方处理，不自动输出可能含业务参数的 SQL 日志。
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	}
	db, err := gorm.Open(dialect, ormConfig)
	if err != nil {
		return nil, fmt.Errorf("初始化 GORM 失败: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取 GORM 连接池失败: %w", err)
	}
	// 这些是本地开发的连接预算，不能当成已测出的系统容量。
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(3 * time.Minute)

	// 初始化不自动发起查询；实际连接检查有 5 秒预算，也接受调用方取消。
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = sqlDB.PingContext(checkCtx)
	if err != nil {
		closeErr := sqlDB.Close()
		return nil, errors.Join(fmt.Errorf("连接 MySQL 失败: %w", err), closeErr)
	}
	return db, nil
}

// Close 在 API 服务或迁移命令结束后，关闭 GORM 的底层连接池。
func Close(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("获取 GORM 底层连接池失败: %w", err)
	}
	return sqlDB.Close()
}

// RequiredSchemaVersion 是当前程序需要的结构版本，新增迁移时同步更新。
const RequiredSchemaVersion uint64 = 6

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

	query = db.WithContext(ctx).Table("live_rooms")
	query = query.Select("id", "owner_id", "title", "status", "created_at", "closed_at").Limit(0)
	rows, err = query.Rows()
	if err != nil {
		return fmt.Errorf("检查 live_rooms 表失败: %w", err)
	}
	if err = rows.Close(); err != nil {
		return fmt.Errorf("关闭 live_rooms 表检查结果失败: %w", err)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("读取 live_rooms 表检查结果失败: %w", err)
	}
	for _, table := range []string{"live_sessions", "consumer_inbox", "interaction_events", "projection_sessions", "worker_checkpoints", "projection_state"} {
		columns := "*"
		if table == "interaction_events" {
			columns = "click_count"
		}
		if table == "projection_state" {
			columns = "id,epoch,topic_id"
		}
		rows, err := db.WithContext(ctx).Table(table).Select(columns).Limit(0).Rows()
		if err != nil {
			return fmt.Errorf("检查 %s 表失败: %w", table, err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

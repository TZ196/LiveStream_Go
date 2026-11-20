package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/logging"
	"github.com/TZ196/LiveStream_Go/backend/migrations"
)

const usage = `用法：migrate
按版本执行数据库迁移，成功后记录，失败立即停止；重复运行跳过成功版本。
目标数据库须先创建。配置使用 LIVESTREAM_MYSQL_ADDR、LIVESTREAM_MYSQL_DATABASE、
LIVESTREAM_MYSQL_USER、LIVESTREAM_MYSQL_PASSWORD；不会自动读取 .env 文件。
`

func main() {
	args := os.Args[1:]
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print(usage)
		return
	}
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "迁移命令不接受业务参数，请使用 -h 查看帮助")
		os.Exit(1)
	}
	logger := logging.New(os.Stdout, slog.LevelInfo)
	logger = logger.With("service", "migrate")
	cfg, err := config.LoadMySQL()
	if err != nil {
		logger.Error("config_invalid", "error", err)
		os.Exit(1)
	}
	err = run(cfg, logger)
	if err != nil {
		logger.Error("migration_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migration_complete", "version", database.RequiredSchemaVersion)
}

func run(cfg config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := database.Open(ctx, cfg)
	if err != nil {
		return err
	}
	// 迁移只由这个入口主动调用，API 启动不会修改表结构。
	migrationErr := migrations.Run(ctx, db, logger)
	closeErr := database.Close(db)
	return errors.Join(migrationErr, closeErr)
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/auth"
	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/httpserver"
	"github.com/TZ196/LiveStream_Go/backend/internal/logging"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
)

func main() {
	// 配置读取失败时也需要记录日志，此时先使用默认日志级别。
	startupLogger := logging.New(os.Stdout, slog.LevelInfo)
	startupLogger = startupLogger.With("service", "api", "stage", httpserver.Stage)

	// os.Args 的第一个元素是程序名，从第二个元素开始才是启动参数。
	cfg, err := config.Load(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		fmt.Print(config.Usage)
		return
	}
	if err != nil {
		startupLogger.Error("config_invalid", "error", err)
		os.Exit(1)
	}

	// 配置有效后，按指定级别建立正式日志，再组装和启动服务。
	logger := logging.New(os.Stdout, cfg.LogLevel)
	logger = logger.With("service", "api", "stage", httpserver.Stage)
	logging.ConfigureHertz(cfg.LogLevel)

	err = run(cfg, logger)
	if err != nil {
		logger.Error("api_failed", "error", err)
		os.Exit(1)
	}
}

// run 管理数据库连接与 HTTP 服务的启动、运行和退出，失败时把错误交给 main。
func run(cfg config.Config, logger *slog.Logger) error {
	// 停止信号也要能取消数据库初始化，避免启动期间一直等待连接。
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 连接和结构检查共用 5 秒预算；迁移命令则只连接，不要求表已经存在。
	startupCtx, cancel := context.WithTimeout(runCtx, 5*time.Second)
	db, err := database.Open(startupCtx, cfg)
	if err != nil {
		cancel()
		if runCtx.Err() != nil {
			logger.Info("startup_cancelled")
			return nil
		}
		return err
	}
	err = database.CheckSchema(startupCtx, db)
	cancel()
	if err != nil {
		closeErr := database.Close(db)
		if runCtx.Err() != nil {
			logger.Info("startup_cancelled")
			return closeErr
		}
		return errors.Join(err, closeErr)
	}
	// 在入口组装业务依赖：Repository 使用 GORM，Service 使用 Repository。
	userRepository := user.NewRepository(db)
	userService, err := user.NewService(userRepository)
	if err != nil {
		return errors.Join(err, database.Close(db))
	}
	tokens := auth.NewTokens(cfg.JWTSecret)
	roomRepository := room.NewRepository(db)
	roomService := room.NewService(roomRepository)
	apiServer := httpserver.New(cfg, logger, db, userService, roomService, tokens)

	// Hertz 会调用这个函数等待运行错误或停止信号。
	// Spin 不直接返回错误，因此需要保存监听失败的原因。
	var serveErr error
	waitForShutdown := func(serverErrors chan error) error {
		select {
		case err := <-serverErrors:
			serveErr = err
		case <-runCtx.Done():
			logger.Info("shutdown_requested")
		}

		// 先标记为停止中，再让 Hertz 执行关闭流程。
		apiServer.MarkStopping()
		return serveErr
	}
	apiServer.Hertz.SetCustomSignalWaiter(waitForShutdown)

	// MySQL 连接和迁移版本检查成功后，才允许进入就绪状态。
	apiServer.MarkReady()
	logger.Info("api_starting",
		"addr", cfg.HTTPAddr,
		"log_level", cfg.LogLevel.String(),
		"shutdown_timeout", cfg.ShutdownTimeout.String(),
	)
	apiServer.Hertz.Spin()

	// HTTP 关闭流程返回后再关闭连接池；关闭错误也交给入口处理。
	closeErr := database.Close(db)
	if closeErr != nil {
		return errors.Join(serveErr, fmt.Errorf("关闭 MySQL 连接池失败: %w", closeErr))
	}
	if serveErr != nil {
		return serveErr
	}
	logger.Info("api_stopped")
	return nil
}

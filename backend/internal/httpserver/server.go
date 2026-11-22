package httpserver

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/auth"
	"github.com/TZ196/LiveStream_Go/backend/internal/config"
	"github.com/TZ196/LiveStream_Go/backend/internal/database"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"gorm.io/gorm"
)

const Stage = "P2"

const (
	stateStarting int32 = 0
	stateReady    int32 = 1
	stateStopping int32 = 2
)

// Server 保存 Hertz 实例、日志工具、GORM 数据库实例和当前就绪状态。
type Server struct {
	Hertz  *server.Hertz
	logger *slog.Logger
	db     *gorm.DB
	users  *user.Service
	rooms  *room.Service
	tokens *auth.Tokens
	limit  *authLimiter
	// 请求处理和退出流程会同时访问状态，使用原子变量避免并发读写冲突。
	state atomic.Int32
}

// New 使用已校验的配置组装 HTTP 服务；开始监听由入口调用 Spin 完成。
func New(cfg config.Config, logger *slog.Logger, db *gorm.DB, users *user.Service, rooms *room.Service, tokens *auth.Tokens) *Server {
	hertzServer := server.New(
		server.WithHostPorts(cfg.HTTPAddr),
		// 标准网络实现会返回监听错误，使端口冲突能由入口统一处理。
		server.WithTransport(standard.NewTransporter),
		server.WithHandleMethodNotAllowed(true),
		server.WithExitWaitTime(cfg.ShutdownTimeout),
		// 当前只接收小型 JSON 请求，限制业务请求占用的内存和读等待。
		server.WithMaxRequestBodySize(8*1024),
		server.WithReadTimeout(5*time.Second),
	)
	apiServer := &Server{
		Hertz:  hertzServer,
		logger: logger,
		db:     db,
		users:  users,
		rooms:  rooms,
		tokens: tokens,
		limit:  &authLimiter{counts: make(map[string]int)},
	}

	// 中间件记录每次请求；两个探针分别报告存活和就绪状态。
	hertzServer.Use(apiServer.accessLog)
	hertzServer.GET("/healthz", health)
	hertzServer.GET("/readyz", apiServer.readiness)
	api := hertzServer.Group("/api/v1", apiServer.businessRequest)
	accounts := api.Group("/auth", apiServer.limitAuth)
	accounts.POST("/register", apiServer.register)
	accounts.POST("/login", apiServer.login)
	api.GET("/users/me", apiServer.authenticate, apiServer.currentUser)
	roomsAPI := api.Group("/rooms", apiServer.authenticate)
	roomsAPI.POST("", apiServer.createRoom)
	roomsAPI.GET("", apiServer.listRooms)
	roomsAPI.GET("/:id", apiServer.getRoom)
	roomsAPI.POST("/:id/close", apiServer.closeRoom)
	roomsAPI.POST("/:id/reopen", apiServer.reopenRoom)
	return apiServer
}

// MarkReady 只允许从初始化进入就绪，停止后不能被迟到的通知重新改为就绪。
func (s *Server) MarkReady() {
	s.state.CompareAndSwap(stateStarting, stateReady)
}

// MarkStopping 在关闭服务前调用，也允许重复调用。
func (s *Server) MarkStopping() {
	s.state.Store(stateStopping)
}

type healthResponse struct {
	Status string `json:"status"`
}

// health 表示进程还能响应请求，不检查数据库等外部依赖。
func health(_ context.Context, c *app.RequestContext) {
	response := healthResponse{Status: "ok"}
	c.Header("Cache-Control", "no-store")
	c.JSON(consts.StatusOK, response)
}

type readinessResponse struct {
	Status       string            `json:"status"`
	Stage        string            `json:"stage"`
	Dependencies map[string]string `json:"dependencies"`
	Recovery     string            `json:"recovery"`
}

// readiness 根据生命周期和数据库检查结果返回 200 或 503。
func (s *Server) readiness(ctx context.Context, c *app.RequestContext) {
	status := "starting"
	statusCode := consts.StatusServiceUnavailable
	mysqlStatus := "not_checked"

	switch s.state.Load() {
	case stateReady:
		// 每次探针执行真实的只读检查，数据库停机后不会继续报告就绪。
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := database.CheckSchema(checkCtx, s.db)
		cancel()
		status = "not_ready"
		mysqlStatus = "unavailable"
		if err == nil {
			status = "ready"
			mysqlStatus = "ready"
			statusCode = consts.StatusOK
		}
	case stateStopping:
		status = "stopping"
	}

	// 数据库检查期间可能收到停止信号，返回前再次确认生命周期。
	if s.state.Load() == stateStopping {
		status = "stopping"
		statusCode = consts.StatusServiceUnavailable
	}
	// 仅返回依赖状态，不向客户端暴露数据库地址、账号或原始错误。
	response := readinessResponse{
		Status:       status,
		Stage:        Stage,
		Dependencies: map[string]string{"mysql": mysqlStatus},
		Recovery:     "not_applicable",
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(statusCode, response)
}

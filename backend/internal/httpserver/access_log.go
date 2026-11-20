package httpserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

// accessLog 在接口处理完成后记录状态码和服务端处理耗时。
func (s *Server) accessLog(ctx context.Context, c *app.RequestContext) {
	start := time.Now()
	// 先执行后续处理，返回后才能取得最终响应状态。
	c.Next(ctx)

	// 只记录注册的路由模板，避免把原始路径、票据或请求内容写入日志。
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}

	status := c.Response.StatusCode()
	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelError
	} else if status >= 400 {
		level = slog.LevelWarn
	}

	method := string(c.Method())
	duration := time.Since(start)
	durationMS := float64(duration.Microseconds()) / 1000
	s.logger.Log(ctx, level, "http_request",
		"trace_id", traceID(c),
		"method", method,
		"route", route,
		"status", status,
		"duration_ms", durationMS,
	)
}

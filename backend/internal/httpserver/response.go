package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

type successResponse struct {
	Data    any    `json:"data"`
	TraceID string `json:"trace_id"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type errorResponse struct {
	Error   errorDetail `json:"error"`
	TraceID string      `json:"trace_id"`
}

func traceID(c *app.RequestContext) string {
	value, _ := c.Get("trace_id")
	id, _ := value.(string)
	return id
}

func writeData(c *app.RequestContext, status int, data any) {
	c.JSON(status, successResponse{Data: data, TraceID: traceID(c)})
}

func writeAPIError(c *app.RequestContext, status int, code, message string, retryable bool) {
	response := errorResponse{
		Error:   errorDetail{Code: code, Message: message, Retryable: retryable},
		TraceID: traceID(c),
	}
	c.AbortWithStatusJSON(status, response)
}

func (s *Server) businessRequest(ctx context.Context, c *app.RequestContext) {
	c.Header("Cache-Control", "no-store")
	var random [16]byte
	_, err := rand.Read(random[:])
	if err != nil {
		writeAPIError(c, 500, "INTERNAL_ERROR", "服务暂时无法处理请求", false)
		return
	}
	id := hex.EncodeToString(random[:])
	c.Set("trace_id", id)
	c.Header("X-Trace-ID", id)
	// 业务数据库查询受本次请求的预算控制；不启动脱离请求的后台任务。
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c.Next(requestCtx)
}

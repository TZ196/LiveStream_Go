package logging

import (
	"io"
	"log/slog"

	"github.com/cloudwego/hertz/pkg/common/hlog"
)

// New 创建 JSON 日志工具，只输出达到指定级别的记录。
func New(output io.Writer, level slog.Level) *slog.Logger {
	options := &slog.HandlerOptions{Level: level}
	handler := slog.NewJSONHandler(output, options)
	logger := slog.New(handler)
	return logger
}

// ConfigureHertz 在启动前设置框架日志级别；框架日志仍使用原生文本格式。
func ConfigureHertz(level slog.Level) {
	hertzLevel := hlog.LevelInfo
	switch level {
	case slog.LevelDebug:
		hertzLevel = hlog.LevelDebug
	case slog.LevelWarn:
		hertzLevel = hlog.LevelWarn
	case slog.LevelError:
		hertzLevel = hlog.LevelError
	}
	hlog.SetLevel(hertzLevel)
}

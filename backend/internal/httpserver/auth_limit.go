package httpserver

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

// 每个整分钟窗口内，同一来源 IP 的注册和登录合计最多 10 次。
// IP 记录也设上限；窗口切换时替换整张表，无需常驻清理 goroutine。
type authLimiter struct {
	mu     sync.Mutex
	window int64
	counts map[string]int
}

func (l *authLimiter) allow(ip string) (bool, int) {
	now := time.Now().Unix()
	l.mu.Lock()
	defer l.mu.Unlock()
	window := now / 60
	if l.window != window {
		l.window = window
		l.counts = make(map[string]int)
	}
	count, exists := l.counts[ip]
	if count >= 10 || (!exists && len(l.counts) >= 10000) {
		return false, int(60 - now%60)
	}
	l.counts[ip] = count + 1
	return true, 0
}

func (s *Server) limitAuth(ctx context.Context, c *app.RequestContext) {
	// 当前没有可信代理配置，只取 TCP 对端地址，不信任客户端伪造的转发头。
	ip, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "无法识别请求来源", false)
		return
	}
	allowed, retryAfter := s.limit.allow(ip)
	if !allowed {
		c.Header("Retry-After", strconv.Itoa(retryAfter))
		writeAPIError(c, 429, "RATE_LIMITED", "登录或注册尝试较多，请稍后再试", true)
		return
	}
	c.Next(ctx)
}

package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/realtime"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
)

// EnableRealtime 注册一次性票据与 WebSocket 路由，调用时机为服务开始监听前。
func (s *Server) EnableRealtime(manager *realtime.Manager, origin string) {
	s.realtime = manager
	s.wsOrigin = origin
	s.Hertz.GET("/metrics", adaptor.HertzHandler(manager.MetricsHandler()))
	limit := &watchTicketLimiter{}
	s.Hertz.POST("/api/v1/rooms/:id/ws-ticket", s.businessRequest, s.authenticate, limit.handle, s.issueWSTicket)
	s.Hertz.GET("/api/v1/rooms/:id/stats", s.businessRequest, s.authenticate, s.roomStats)
	s.Hertz.GET("/api/v1/rooms/:id/sessions/:session/like-leaderboard", s.businessRequest, s.authenticate, s.likeLeaderboard)
	s.Hertz.GET("/ws/rooms/:id", adaptor.HertzHandler(http.HandlerFunc(manager.ServeHTTP)))
}

// 认证后、查数据库前限制票据请求，限制器本身也有容量上限。
type watchTicketLimiter struct {
	mu     sync.Mutex
	window int64
	counts map[uint64]int
}

func (l *watchTicketLimiter) handle(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	retryAfter := l.retryAfter(identity.UserID, time.Now().Unix())
	if retryAfter > 0 {
		c.Header("Retry-After", strconv.FormatInt(retryAfter, 10))
		writeAPIError(c, 429, "WS_TICKET_RATE_LIMITED", "进入直播间过于频繁，请稍后再试", true)
		return
	}
	c.Next(ctx)
}

func (l *watchTicketLimiter) retryAfter(userID uint64, now int64) int64 {
	const windowSeconds = 10
	l.mu.Lock()
	defer l.mu.Unlock()
	window := now / windowSeconds
	if l.counts == nil || l.window != window {
		l.window = window
		l.counts = make(map[uint64]int)
	}
	count, exists := l.counts[userID]
	if count >= 6 || (!exists && len(l.counts) >= 10000) {
		return windowSeconds - now%windowSeconds
	}
	l.counts[userID] = count + 1
	return 0
}

// issueWSTicket 为已认证用户签发绑定房间、场次和来源的短期票据。
func (s *Server) issueWSTicket(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	roomID, ok := readRoomID(c)
	if !ok {
		return
	}
	var input struct {
		Mode    string `json:"mode"`
		WatchID string `json:"watch_id"`
	}
	if !readRoomObject(c, &input) {
		return
	}
	origin := string(c.Request.Header.Peek("Origin"))
	if origin == "" || !strings.EqualFold(origin, s.wsOrigin) {
		writeAPIError(c, 403, "ORIGIN_REJECTED", "访问来源不被允许", false)
		return
	}
	session, err := s.rooms.ActiveSession(ctx, roomID)
	if errors.Is(err, room.ErrNotFound) || errors.Is(err, room.ErrUnavailable) {
		s.roomError(ctx, c, err)
		return
	}
	if err != nil {
		writeAPIError(c, 409, "ROOM_CLOSED", "直播间暂不可加入", false)
		return
	}
	encoded, expires, watchID, err := s.realtime.IssueTicket(ctx, identity, roomID, session.ID, origin, input.Mode, input.WatchID)
	if err != nil {
		switch {
		case errors.Is(err, realtime.ErrInvalidWatch):
			writeAPIError(c, 400, "INVALID_ARGUMENT", "请指定进入或恢复观看，恢复时提供观看编号", false)
		case errors.Is(err, realtime.ErrWatchReplaced):
			writeAPIError(c, 409, "CONNECTION_REPLACED", "你的账号已在其他窗口进入直播间", false)
		case errors.Is(err, realtime.ErrWatchExpired):
			writeAPIError(c, 409, "WATCH_EXPIRED", "观看连接已失效，请重新进入", false)
		case errors.Is(err, realtime.ErrTicketCapacity):
			c.Header("Retry-After", "1")
			writeAPIError(c, 503, "CAPACITY_EXCEEDED", "当前连接较多，请稍后再试", true)
		default:
			c.Header("Retry-After", "1")
			writeAPIError(c, 503, "WS_OWNERSHIP_UNAVAILABLE", "直播连接暂不可用，请稍后再试", true)
		}
		return
	}
	writeData(c, 201, map[string]any{"ticket": encoded, "expires_at": expires.UTC().Format(time.RFC3339Nano), "ws_path": fmt.Sprintf("/ws/rooms/%d?ticket=%s", roomID, encoded), "session_id": fmt.Sprint(session.ID), "watch_id": watchID})
}

func (s *Server) roomStats(ctx context.Context, c *app.RequestContext) {
	roomID, ok := readRoomID(c)
	if !ok {
		return
	}
	if c.QueryArgs().Len() != 0 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "统计接口不接受查询参数", false)
		return
	}
	value, err := s.rooms.Get(ctx, roomID)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	var sessionID uint64
	if value.Status == room.Open {
		session, err := s.rooms.ActiveSession(ctx, roomID)
		if errors.Is(err, room.ErrNotFound) || errors.Is(err, room.ErrUnavailable) {
			s.roomError(ctx, c, err)
			return
		}
		if err != nil {
			writeAPIError(c, 409, "SESSION_CHANGED", "直播场次已经结束或改变", true)
			return
		}
		sessionID = session.ID
	}
	writeData(c, 200, s.realtime.RoomStats(ctx, roomID, sessionID))
}

// likesState 只读 MySQL 确认场次，Redis 不可用时首次进入的页面也能暂存点击。
func (s *Server) likesState(ctx context.Context, c *app.RequestContext) {
	roomID, ok := readRoomID(c)
	if !ok {
		return
	}
	value, err := s.rooms.Get(ctx, roomID)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	state := "BLOCKED"
	if s.realtime != nil {
		state = s.realtime.State()
	}
	var sessionID any
	if value.Status == room.Open {
		session, err := s.rooms.ActiveSession(ctx, roomID)
		if err != nil {
			s.roomError(ctx, c, err)
			return
		}
		sessionID = strconv.FormatUint(session.ID, 10)
	}
	writeData(c, 200, map[string]any{"state": state, "session_id": sessionID, "room_status": value.Status})
}

// likeLeaderboard 验证当前房间与场次，仅使用登录身份查询个人部分。
func (s *Server) likeLeaderboard(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	roomID, ok := readRoomID(c)
	if !ok {
		return
	}
	raw := c.Param("session")
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != raw || c.QueryArgs().Len() != 0 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "场次编号或查询参数无效", false)
		return
	}
	session, err := s.rooms.ActiveSession(ctx, roomID)
	if errors.Is(err, room.ErrNotFound) {
		writeAPIError(c, 404, "ROOM_NOT_FOUND", "房间不存在", false)
		return
	}
	if errors.Is(err, room.ErrUnavailable) {
		writeAPIError(c, 503, "DEPENDENCY_UNAVAILABLE", "暂时无法查询房间", true)
		return
	}
	if err != nil || session.ID != id {
		writeAPIError(c, 409, "SESSION_CHANGED", "直播场次已经结束或改变", false)
		return
	}
	board, err := s.realtime.Leaderboard(ctx, roomID, id, identity.UserID)
	if err != nil {
		writeAPIError(c, 503, "LEADERBOARD_UNAVAILABLE", "本场点赞榜暂不可用", true)
		return
	}
	writeData(c, 200, board)
}

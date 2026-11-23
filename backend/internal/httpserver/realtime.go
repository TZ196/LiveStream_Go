package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
	s.Hertz.POST("/api/v1/rooms/:id/ws-ticket", s.businessRequest, s.authenticate, s.issueWSTicket)
	s.Hertz.GET("/api/v1/rooms/:id/sessions/:session/like-leaderboard", s.businessRequest, s.authenticate, s.likeLeaderboard)
	s.Hertz.GET("/ws/rooms/:id", adaptor.HertzHandler(http.HandlerFunc(manager.ServeHTTP)))
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
	var input struct{}
	if !readRoomObject(c, &input) {
		return
	}
	origin := string(c.Request.Header.Peek("Origin"))
	if origin == "" || !strings.EqualFold(origin, s.wsOrigin) {
		writeAPIError(c, 403, "ORIGIN_REJECTED", "访问来源不被允许", false)
		return
	}
	session, err := s.rooms.ActiveSession(ctx, roomID)
	if err != nil {
		writeAPIError(c, 409, "ROOM_CLOSED", "直播间暂不可加入", false)
		return
	}
	encoded, expires, err := s.realtime.Tickets.Issue(identity, roomID, session.ID, origin)
	if err != nil {
		writeAPIError(c, 503, "CAPACITY_EXCEEDED", "当前连接较多，请稍后再试", true)
		return
	}
	writeData(c, 201, map[string]any{"ticket": encoded, "expires_at": expires.UTC().Format(time.RFC3339Nano), "ws_path": fmt.Sprintf("/ws/rooms/%d?ticket=%s", roomID, encoded), "session_id": fmt.Sprint(session.ID)})
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

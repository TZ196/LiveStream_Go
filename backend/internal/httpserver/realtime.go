package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/realtime"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
)

// EnableRealtime 注册一次性票据与 WebSocket 路由，调用时机为服务开始监听前。
func (s *Server) EnableRealtime(manager *realtime.Manager, origin string) {
	s.realtime = manager
	s.wsOrigin = origin
	s.Hertz.POST("/api/v1/rooms/:id/ws-ticket", s.businessRequest, s.authenticate, s.issueWSTicket)
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

package httpserver

import (
	"context"
	"strconv"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/cloudwego/hertz/pkg/app"
)

// EnableDanmakuHistory 在监听前注册；审核词库损坏不影响已落库历史查询。
func (s *Server) EnableDanmakuHistory(repository *danmaku.Repository) {
	s.danmaku = repository
	s.Hertz.GET("/api/v1/rooms/:id/danmaku", s.businessRequest, s.authenticate, s.danmakuHistory)
}

func (s *Server) danmakuHistory(ctx context.Context, c *app.RequestContext) {
	roomID, ok := readRoomID(c)
	if !ok {
		return
	}
	if _, err := s.rooms.Get(ctx, roomID); err != nil {
		s.roomError(ctx, c, err)
		return
	}
	query := c.QueryArgs()
	invalid := false
	query.VisitAll(func(key, value []byte) {
		name := string(key)
		if name != "session_id" && name != "limit" && name != "cursor" {
			invalid = true
		}
		if len(query.PeekAll(name)) != 1 || len(value) == 0 {
			invalid = true
		}
	})
	if invalid {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "历史查询参数无效", false)
		return
	}
	limit := 20
	var err error
	if raw := string(query.Peek("limit")); raw != "" {
		limit, err = strconv.Atoi(raw)
	}
	if err != nil || limit < 1 || limit > 100 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "每页数量须为 1 至 100", false)
		return
	}
	var sessionID uint64
	if raw := string(query.Peek("session_id")); raw != "" {
		sessionID, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || sessionID == 0 || strconv.FormatUint(sessionID, 10) != raw {
			writeAPIError(c, 400, "INVALID_ARGUMENT", "场次编号无效", false)
			return
		}
		valid, checkErr := s.danmaku.SessionBelongs(ctx, roomID, sessionID)
		if checkErr != nil {
			writeAPIError(c, 503, "DEPENDENCY_UNAVAILABLE", "暂时无法查询场次", true)
			return
		}
		if !valid {
			writeAPIError(c, 404, "ROOM_NOT_FOUND", "房间中不存在该场次", false)
			return
		}
	}
	result, err := s.danmaku.History(ctx, roomID, sessionID, limit, string(query.Peek("cursor")))
	if err != nil {
		fault := danmaku.AsFault(err)
		status := 400
		if fault.Retryable {
			status = 503
		}
		writeAPIError(c, status, fault.Code, fault.Message, fault.Retryable)
		return
	}
	writeData(c, 200, result)
}

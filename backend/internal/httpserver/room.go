package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/TZ196/LiveStream_Go/backend/internal/auth"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/cloudwego/hertz/pkg/app"
)

type roomResponse struct {
	ID        string  `json:"id"`
	OwnerID   string  `json:"owner_id"`
	Title     string  `json:"title"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"created_at"`
	ClosedAt  *string `json:"closed_at"`
}

func publicRoom(value room.Room) roomResponse {
	response := roomResponse{
		ID:        strconv.FormatUint(value.ID, 10),
		OwnerID:   strconv.FormatUint(value.OwnerID, 10),
		Title:     value.Title,
		Status:    value.Status,
		CreatedAt: value.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	if value.ClosedAt != nil {
		closedAt := value.ClosedAt.UTC().Format("2006-01-02T15:04:05.000Z")
		response.ClosedAt = &closedAt
	}
	return response
}

func roomIdentity(c *app.RequestContext) (auth.Identity, bool) {
	value, _ := c.Get("identity")
	identity, ok := value.(auth.Identity)
	if !ok {
		writeAPIError(c, 401, "UNAUTHORIZED", "请先登录", false)
	}
	return identity, ok
}

func readRoomID(c *app.RequestContext) (uint64, bool) {
	value := c.Param("id")
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != value {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "房间编号须为正整数", false)
		return 0, false
	}
	return id, true
}

func readRoomObject(c *app.RequestContext, input any) bool {
	mediaType, params, err := mime.ParseMediaType(string(c.Request.Header.ContentType()))
	charset := params["charset"]
	if err != nil || mediaType != "application/json" || (charset != "" && !strings.EqualFold(charset, "utf-8")) {
		writeAPIError(c, 415, "UNSUPPORTED_MEDIA_TYPE", "请使用 UTF-8 的 application/json 请求", false)
		return false
	}
	body := bytes.TrimSpace(c.Request.Body())
	if c.QueryArgs().Len() != 0 || !utf8.Valid(body) || len(body) == 0 || body[0] != '{' {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "请求须为单个 JSON 对象，且不接受查询参数", false)
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(input); err != nil {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "请求字段或格式不正确", false)
		return false
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "请求只能包含一个 JSON 对象", false)
		return false
	}
	return true
}

func (s *Server) createRoom(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	var input struct {
		Title string `json:"title"`
	}
	if !readRoomObject(c, &input) {
		return
	}
	value, err := s.rooms.Create(ctx, identity.UserID, input.Title)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	writeData(c, 201, publicRoom(value))
}

func (s *Server) getRoom(ctx context.Context, c *app.RequestContext) {
	id, ok := readRoomID(c)
	if !ok {
		return
	}
	if c.QueryArgs().Len() != 0 || len(c.Request.Body()) != 0 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "房间详情不接受查询参数或请求体", false)
		return
	}
	value, err := s.rooms.Get(ctx, id)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	writeData(c, 200, publicRoom(value))
}

func (s *Server) listRooms(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	invalid := len(c.Request.Body()) != 0
	seen := make(map[string]bool)
	// Hertz 遍历查询参数时调用此回调，拒绝未知字段和重复参数。
	c.QueryArgs().VisitAll(func(key, _ []byte) {
		name := string(key)
		if seen[name] || (name != "status" && name != "owner" && name != "limit" && name != "cursor") {
			invalid = true
		}
		seen[name] = true
	})
	if invalid {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "查询参数无效、重复或包含请求体", false)
		return
	}
	options := room.ListOptions{Status: room.Open, Limit: 20, Cursor: c.Query("cursor")}
	if seen["status"] {
		options.Status = c.Query("status")
	}
	if seen["owner"] {
		if c.Query("owner") != "me" {
			writeAPIError(c, 400, "INVALID_ARGUMENT", "owner 只允许为 me", false)
			return
		}
		options.OwnerID = identity.UserID
	}
	if seen["limit"] {
		limit, err := strconv.Atoi(c.Query("limit"))
		if err != nil {
			writeAPIError(c, 400, "INVALID_ARGUMENT", "每页条数须为 1–100 的整数", false)
			return
		}
		options.Limit = limit
	}
	values, next, err := s.rooms.List(ctx, options)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	items := make([]roomResponse, 0, len(values))
	for _, value := range values {
		items = append(items, publicRoom(value))
	}
	response := struct {
		Items      []roomResponse `json:"items"`
		NextCursor *string        `json:"next_cursor"`
	}{Items: items, NextCursor: next}
	writeData(c, 200, response)
}

func (s *Server) closeRoom(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	id, ok := readRoomID(c)
	if !ok {
		return
	}
	var input struct{}
	if !readRoomObject(c, &input) {
		return
	}
	value, err := s.rooms.Close(ctx, id, identity.UserID)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	writeData(c, 200, publicRoom(value))
}

func (s *Server) reopenRoom(ctx context.Context, c *app.RequestContext) {
	identity, ok := roomIdentity(c)
	if !ok {
		return
	}
	id, ok := readRoomID(c)
	if !ok {
		return
	}
	var input struct{}
	if !readRoomObject(c, &input) {
		return
	}
	value, err := s.rooms.Reopen(ctx, id, identity.UserID)
	if err != nil {
		s.roomError(ctx, c, err)
		return
	}
	writeData(c, 200, publicRoom(value))
}

func (s *Server) roomError(ctx context.Context, c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, room.ErrInvalidTitle), errors.Is(err, room.ErrInvalidList):
		writeAPIError(c, 400, "INVALID_ARGUMENT", err.Error(), false)
	case errors.Is(err, room.ErrInvalidCursor):
		writeAPIError(c, 400, "INVALID_CURSOR", err.Error(), false)
	case errors.Is(err, room.ErrNotFound):
		writeAPIError(c, 404, "ROOM_NOT_FOUND", err.Error(), false)
	case errors.Is(err, room.ErrForbidden):
		writeAPIError(c, 403, "FORBIDDEN", err.Error(), false)
	case errors.Is(err, room.ErrUnavailable):
		c.Header("Retry-After", "1")
		s.logger.ErrorContext(ctx, "room_dependency_unavailable", "trace_id", traceID(c))
		writeAPIError(c, 503, "DEPENDENCY_UNAVAILABLE", "服务暂不可用，请稍后确认房间状态", true)
	default:
		s.logger.ErrorContext(ctx, "room_operation_failed", "trace_id", traceID(c))
		writeAPIError(c, 500, "INTERNAL_ERROR", "服务暂时无法处理请求", false)
	}
}

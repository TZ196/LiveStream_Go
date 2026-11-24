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

	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/cloudwego/hertz/pkg/app"
)

type credentialsRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type userResponse struct {
	ID        string `json:"id"`
	Username  string `json:"username"`
	CreatedAt string `json:"created_at"`
}

func publicUser(account user.User) userResponse {
	return userResponse{
		ID: strconv.FormatUint(account.ID, 10), Username: account.Username,
		CreatedAt: account.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

func readCredentials(c *app.RequestContext) (credentialsRequest, bool) {
	var input credentialsRequest
	mediaType, params, err := mime.ParseMediaType(string(c.Request.Header.ContentType()))
	charset := params["charset"]
	if err != nil || mediaType != "application/json" || (charset != "" && !strings.EqualFold(charset, "utf-8")) {
		writeAPIError(c, 415, "UNSUPPORTED_MEDIA_TYPE", "请使用 UTF-8 的 application/json 请求", false)
		return input, false
	}
	body := c.Request.Body()
	if !utf8.Valid(body) || c.QueryArgs().Len() != 0 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "请求须为 UTF-8 JSON，且不接受查询参数", false)
		return input, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&input)
	if err != nil {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "请求须包含 username 和 password，不能包含其他字段", false)
		return input, false
	}
	// 只允许一个 JSON 对象，拒绝在合法对象后继续拼接内容。
	var trailing any
	err = decoder.Decode(&trailing)
	if err != io.EOF || input.Username == "" || input.Password == "" {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "用户名和密码不能为空，请检查 JSON 请求", false)
		return input, false
	}
	return input, true
}

func (s *Server) register(ctx context.Context, c *app.RequestContext) {
	input, ok := readCredentials(c)
	if !ok {
		return
	}
	account, err := s.users.Register(ctx, input.Username, input.Password)
	if err != nil {
		s.userError(ctx, c, err)
		return
	}
	writeData(c, 201, publicUser(account))
}

func (s *Server) login(ctx context.Context, c *app.RequestContext) {
	input, ok := readCredentials(c)
	if !ok {
		return
	}
	account, err := s.users.Login(ctx, input.Username, input.Password)
	if err != nil {
		s.userError(ctx, c, err)
		return
	}
	encoded, err := s.tokens.Issue(account.ID)
	if err != nil {
		s.userError(ctx, c, err)
		return
	}
	response := struct {
		AccessToken string       `json:"access_token"`
		TokenType   string       `json:"token_type"`
		ExpiresIn   int          `json:"expires_in"`
		User        userResponse `json:"user"`
	}{encoded, "Bearer", int(user.Lifetime.Seconds()), publicUser(account)}
	writeData(c, 200, response)
}

func (s *Server) authenticate(ctx context.Context, c *app.RequestContext) {
	headers := c.Request.Header.PeekAll("Authorization")
	if len(headers) != 1 {
		writeAPIError(c, 401, "UNAUTHORIZED", "请先登录", false)
		return
	}
	scheme, encoded, found := strings.Cut(string(headers[0]), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		writeAPIError(c, 401, "UNAUTHORIZED", "请使用 Bearer 登录凭证", false)
		return
	}
	identity, err := s.tokens.Parse(encoded)
	if err != nil {
		code := "UNAUTHORIZED"
		if errors.Is(err, user.ErrTokenExpired) {
			code = "TOKEN_EXPIRED"
		}
		writeAPIError(c, 401, code, err.Error(), false)
		return
	}
	c.Set("identity", identity)
	c.Next(ctx)
}

func (s *Server) currentUser(ctx context.Context, c *app.RequestContext) {
	if c.QueryArgs().Len() != 0 || len(c.Request.Body()) != 0 {
		writeAPIError(c, 400, "INVALID_ARGUMENT", "当前用户接口不接受用户 ID 或请求体", false)
		return
	}
	value, _ := c.Get("identity")
	identity, ok := value.(user.Identity)
	if !ok {
		writeAPIError(c, 401, "UNAUTHORIZED", "请先登录", false)
		return
	}
	account, err := s.users.Current(ctx, identity.UserID)
	if err != nil {
		s.userError(ctx, c, err)
		return
	}
	writeData(c, 200, publicUser(account))
}

func (s *Server) userError(ctx context.Context, c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, user.ErrInvalidUsername), errors.Is(err, user.ErrInvalidPassword):
		writeAPIError(c, 400, "INVALID_ARGUMENT", err.Error(), false)
	case errors.Is(err, user.ErrUsernameExists):
		writeAPIError(c, 409, "USERNAME_EXISTS", "用户名已被使用", false)
	case errors.Is(err, user.ErrInvalidCredentials):
		writeAPIError(c, 401, "INVALID_CREDENTIALS", "用户名或密码不正确", false)
	case errors.Is(err, user.ErrNotFound):
		writeAPIError(c, 401, "UNAUTHORIZED", "登录账号不可用，请重新登录", false)
	case errors.Is(err, user.ErrCapacity):
		c.Header("Retry-After", "1")
		writeAPIError(c, 429, "CAPACITY_EXCEEDED", "登录或注册请求较多，请稍后再试", true)
	case errors.Is(err, user.ErrUnavailable):
		c.Header("Retry-After", "1")
		s.logger.ErrorContext(ctx, "user_dependency_unavailable", "trace_id", traceID(c))
		writeAPIError(c, 503, "DEPENDENCY_UNAVAILABLE", "服务暂不可用，请稍后再试", true)
	default:
		// 不记录原始参数、密码哈希或令牌，诊断通过 trace_id 关联请求。
		s.logger.ErrorContext(ctx, "user_operation_failed", "trace_id", traceID(c))
		writeAPIError(c, 500, "INTERNAL_ERROR", "服务暂时无法处理请求", false)
	}
}

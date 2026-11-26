package danmaku

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

// RecentStore 是非权威的五分钟弹幕缓存；丢失不影响 Kafka 广播及持久化。
type RecentStore struct{ client *redis.Client }

func NewRecentStore(client *redis.Client) *RecentStore { return &RecentStore{client: client} }

const recentMaxItems = 300

// 缓存保留时间为 5 分钟加随机 0–60 秒；查询窗口仍严格限制为 5 分钟。
func recentTTLMillis() int {
	return 300000 + rand.IntN(60001)
}

//go:embed lua/recent_add.lua
var addRecentLua string

var addRecent = redis.NewScript(addRecentLua)

var ErrRecentConflict = errors.New("最近弹幕缓存发现事件内容冲突")

// Add 使用事件时间，重复投递不刷新已有成员的分数。
func (store *RecentStore) Add(ctx context.Context, record Record) error {
	// V1 没有可靠的服务端接收时间，不能伪造后放入最近窗口。
	if record.Event.ServerReceivedAt == 0 {
		return nil
	}
	raw, err := json.Marshal(record.Event)
	if err != nil {
		return err
	}
	key := recentKey(record.Event.RoomID, record.Event.SessionID)
	result, err := addRecent.Run(ctx, store.client, []string{key, key + ":lock", key + ":ready"}, record.Event.ServerReceivedAt, string(raw), record.Event.EventID, recentMaxItems, recentTTLMillis()).Int()
	if err != nil {
		return err
	}
	if result == 2 {
		return ErrRecentConflict
	}
	return nil
}

func recentKey(room, session string) string {
	return "danmaku:recent:v2:{" + room + ":" + session + "}"
}

var ErrCacheWarming = errors.New("最近弹幕缓存正在恢复")
var ErrInvalidRecentCursor = errors.New("无效游标")
var ErrRecentSessionMissing = errors.New("房间中不存在该场次")

//go:embed lua/recent_page.lua
var readRecentLua string

var readRecent = redis.NewScript(readRecentLua)

type RecentPage struct {
	Items      []RecentItem `json:"items"`
	NextCursor *string      `json:"next_cursor"`
}
type RecentItem struct {
	EventID   string `json:"event_id"`
	RequestID string `json:"request_id"`
	RoomID    string `json:"room_id"`
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

// Page 通过成员排名分页，包含相同毫秒分数的记录；游标过期后停止翻页。
func (store *RecentStore) Page(ctx context.Context, room, session, cursor string) (RecentPage, error) {
	page := RecentPage{Items: make([]RecentItem, 0, 20)}
	member := ""
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(raw) > 4096 {
			return page, ErrInvalidRecentCursor
		}
		var event Event
		if json.Unmarshal(raw, &event) != nil || event.RoomID != room || event.SessionID != session {
			return page, ErrInvalidRecentCursor
		}
		member = string(raw)
	}
	key := recentKey(room, session)
	values, err := readRecent.Run(ctx, store.client, []string{key, key + ":ready", key + ":lock"}, member).StringSlice()
	if err != nil {
		return page, fmt.Errorf("读取最近弹幕: %w", err)
	}
	if len(values) == 0 || values[0] == "WARMING" {
		return page, ErrCacheWarming
	}
	if values[0] == "MISSING" {
		return page, ErrRecentSessionMissing
	}
	values = values[1:]
	if len(values) == 20 {
		next := base64.RawURLEncoding.EncodeToString([]byte(values[19]))
		page.NextCursor = &next
	}
	for _, raw := range values {
		var event Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil || event.RoomID != room || event.SessionID != session {
			return RecentPage{}, errors.New("最近弹幕缓存损坏")
		}
		page.Items = append(page.Items, RecentItem{event.EventID, event.RequestID, event.RoomID, event.SessionID, event.UserID, event.Payload.Username, event.Payload.Content, time.UnixMilli(event.ServerReceivedAt).UTC().Format("2006-01-02T15:04:05.000Z")})
	}
	return page, nil
}

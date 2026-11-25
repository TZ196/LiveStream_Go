package danmaku

import (
	"context"
	_ "embed"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RequestStore 的记录是准入依据，不将 PROCESSING 当成 Kafka 接受证明。
type RequestStore struct {
	client     *redis.Client
	mu         sync.RWMutex
	generation string
	cutoff     int64
}
type requestState struct{ Hash, Status, Event, Category, Version string }

const identityKey = "danmaku:requests:identity"

//go:embed lua/update_identity.lua
var updateIdentityLua string

var updateIdentity = redis.NewScript(updateIdentityLua)

func NewRequestStore(client *redis.Client) *RequestStore { return &RequestStore{client: client} }
func requestKey(user, request string) string             { return "danmaku:request:" + user + ":" + request }

// 额度按用户和房间隔离；同房间的多个页面以及重新开播仍共享这个十秒窗口。
func rateKey(user, room string) string { return "danmaku:rate:" + user + ":" + room }

// Check 在 Redis 换代或请求账本丢失时建立保护时间。旧记录缺失不能被当作首次请求。
func (store *RequestStore) Check(ctx context.Context, historical bool) error {
	info, err := store.client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	run := ""
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			run = strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
		}
	}
	if run == "" {
		return errors.New("无法识别弹幕 Redis 实例")
	}
	raw, err := store.client.Get(ctx, identityKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !strings.HasPrefix(raw, run+":") {
		clock, err := store.client.Time(ctx).Result()
		if err != nil {
			return err
		}
		cutoff := int64(0)
		if historical || raw != "" || store.generation != "" {
			// 覆盖客户端允许的五分钟未来偏差；保护期内保留草稿，旧请求仍可核对。
			cutoff = clock.Add(5 * time.Minute).UnixMilli()
		}
		next := run + ":" + strconv.FormatInt(cutoff, 10)
		changed, err := updateIdentity.Run(ctx, store.client, []string{identityKey}, raw, next).Int()
		if err != nil {
			return err
		}
		if changed != 1 {
			return errors.New("弹幕请求账本正在重新核对")
		}
		raw = next
	}
	cutoff, err := strconv.ParseInt(strings.TrimPrefix(raw, run+":"), 10, 64)
	if err != nil || cutoff < 0 {
		return errors.New("弹幕请求账本代次无效")
	}
	store.generation, store.cutoff = raw, cutoff
	return nil
}
func (store *RequestStore) NewAllowed() bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.generation != "" && time.Now().UnixMilli() > store.cutoff
}

func (store *RequestStore) Lookup(ctx context.Context, user, request string) (requestState, error) {
	data, err := store.client.HGetAll(ctx, requestKey(user, request)).Result()
	if err != nil {
		return requestState{}, err
	}
	return requestState{Hash: data["hash"], Status: data["status"], Event: data["event"], Category: data["category"], Version: data["version"]}, nil
}

//go:embed lua/begin_request.lua
var beginRequestLua string

var beginRequest = redis.NewScript(beginRequestLua)

func (store *RequestStore) Begin(ctx context.Context, event Event, token string) ([]string, error) {
	store.mu.RLock()
	generation := store.generation
	store.mu.RUnlock()
	return beginRequest.Run(ctx, store.client, []string{requestKey(event.UserID, event.RequestID), rateKey(event.UserID, event.RoomID), "danmaku:requests:ledger", identityKey},
		event.RequestHash, token, event.Timestamp+int64(25*time.Hour/time.Millisecond), event.Timestamp, generation).StringSlice()
}

//go:embed lua/change_request.lua
var changeRequestLua string

var changeRequest = redis.NewScript(changeRequestLua)

func (store *RequestStore) Change(ctx context.Context, event Event, token, action, value, version string) error {
	changed, err := changeRequest.Run(ctx, store.client, []string{requestKey(event.UserID, event.RequestID)}, event.RequestHash, token, action, value, version).Int()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("弹幕请求执行权或冻结记录已改变")
	}
	return nil
}

func (store *RequestStore) Uncertain(timestamp int64) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.cutoff > 0 && timestamp <= store.cutoff
}

// ValidateRealtime 只核对本业务的 Topic 与处理位置；广播没有需要重算的点赞数。
func (store *RequestStore) ValidateRealtime(ctx context.Context, topicID string, committed map[int32]int64) error {
	identity := "danmaku:realtime:topic"
	if err := store.client.SetNX(ctx, identity, topicID, 0).Err(); err != nil {
		return err
	}
	actual, err := store.client.Get(ctx, identity).Result()
	if err != nil {
		return err
	}
	if actual != topicID {
		return ErrBlocked
	}
	for partition, offset := range committed {
		raw, err := store.client.HGet(ctx, "danmaku:realtime:"+topicID, strconv.Itoa(int(partition))).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		saved, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || saved < 0 || (offset < 0 && saved > 0) {
			return ErrBlocked
		}
	}
	return nil
}

//go:embed lua/mark_realtime.lua
var markRealtimeLua string

var markRealtime = redis.NewScript(markRealtimeLua)

func (store *RequestStore) MarkRealtime(ctx context.Context, record Record) error {
	return markRealtime.Run(ctx, store.client, []string{"danmaku:realtime:" + record.TopicID}, strconv.Itoa(int(record.Partition)), strconv.FormatInt(record.NextOffset, 10)).Err()
}

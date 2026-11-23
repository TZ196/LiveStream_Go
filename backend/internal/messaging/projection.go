package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProjectionRepository 负责实时点赞投影；不参与发送前请求登记。
type ProjectionRepository struct {
	client *redis.Client
	epoch  string
}

// NewProjectionRepository 绑定已完成初始化的投影代次。
func NewProjectionRepository(client *redis.Client, epoch string) (*ProjectionRepository, error) {
	if client == nil || epoch == "" {
		return nil, errors.New("Redis 客户端和投影代次必填")
	}
	return &ProjectionRepository{client: client, epoch: epoch}, nil
}

var initializeSessionScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  if redis.call('HGET', KEYS[1], 'room_id') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'initialized') ~= '1' then return redis.error_reply('SESSION_MISMATCH') end
  return 0
end
redis.call('HSET', KEYS[1], 'room_id', ARGV[1], 'likes', '0', 'revision', '0', 'initialized', '1', 'board_users', '0', 'board_revision', '0', 'board_through', '0')
return 1
`)

// InitializeNewSession 只供已确认从未接收过点赞的新场次初始化；禁止用于丢失后的重建。
func (repository *ProjectionRepository) InitializeNewSession(ctx context.Context, roomID, sessionID string) error {
	key := "ls:v1:projection:" + repository.epoch + ":session:" + sessionID
	return initializeSessionScript.Run(ctx, repository.client, []string{key}, roomID).Err()
}

// Lua 脚本一次完成指纹比较、去重和计数；缺少已初始化场次时拒绝处理。
var projectLikeScript = redis.NewScript(leaderboardLua + projectLikeLua)

// ProjectedLike 是处理后的实时场次计数及投影版本。
type ProjectedLike struct {
	Count         string
	Revision      string
	Duplicate     bool
	Personal      string
	BoardRevision string
}

// ApplyLike 原子处理单条消息；返回前未提交 Kafka 位移。
func (repository *ProjectionRepository) ApplyLike(ctx context.Context, record *kgo.Record) (ProjectedLike, error) {
	event, err := DecodeLikeRecord(record)
	if err != nil {
		return ProjectedLike{}, err
	}
	if time.Since(time.UnixMilli(event.Timestamp)) >= 8*24*time.Hour {
		return ProjectedLike{}, errors.New("事件积压超过在线投影安全窗口")
	}
	prefix := "ls:v1:projection:" + repository.epoch + ":"
	sessionKey := repository.sessionKey(event.SessionID)
	keys := []string{sessionKey, prefix + "processed:" + event.EventID, prefix + "partition:" + strconv.Itoa(int(record.Partition)), sessionKey + ":users", sessionKey + ":top"}
	fingerprint := event.UserID + ":" + event.RequestHash
	result, err := projectLikeScript.Run(ctx, repository.client, keys, event.RoomID, fingerprint, 9*24*60*60, record.Offset+1, event.ClickCount(), event.UserID).StringSlice()
	if err != nil {
		return ProjectedLike{}, fmt.Errorf("更新实时点赞投影: %w", err)
	}
	if len(result) != 5 {
		return ProjectedLike{}, errors.New("实时点赞投影响应不完整")
	}
	return ProjectedLike{Count: result[0], Revision: result[1], Duplicate: result[2] == "duplicate", Personal: result[3], BoardRevision: result[4]}, nil
}

// Ping 检查 Redis 是否可以响应，不修改投影。
func (repository *ProjectionRepository) Ping(ctx context.Context) error {
	return repository.client.Ping(ctx).Err()
}

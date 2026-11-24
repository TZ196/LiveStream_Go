package likes

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
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
func (repository *ProjectionRepository) ApplyLike(ctx context.Context, record ConsumedLike) (ProjectedLike, error) {
	event := record.Event
	if time.Since(time.UnixMilli(event.Timestamp)) >= 8*24*time.Hour {
		return ProjectedLike{}, errors.New("事件积压超过在线投影安全窗口")
	}
	prefix := "ls:v1:projection:" + repository.epoch + ":"
	sessionKey := repository.sessionKey(event.SessionID)
	keys := []string{sessionKey, prefix + "processed:" + event.EventID, prefix + "partition:" + strconv.Itoa(int(record.Partition)), sessionKey + ":users", sessionKey + ":top"}
	fingerprint := event.UserID + ":" + event.RequestHash
	result, err := projectLikeScript.Run(ctx, repository.client, keys, event.RoomID, fingerprint, 9*24*60*60, record.NextOffset, event.ClickCount(), event.UserID).StringSlice()
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

// ReadSession 读取已初始化场次计数；缺失或 Redis 不可用不返回伪造的零。
func (repository *ProjectionRepository) ReadSession(ctx context.Context, roomID, sessionID string) (ProjectedLike, error) {
	key := "ls:v1:projection:" + repository.epoch + ":session:" + sessionID
	values, err := repository.client.HMGet(ctx, key, "room_id", "likes", "revision", "initialized").Result()
	if err != nil {
		return ProjectedLike{}, err
	}
	if len(values) != 4 || values[0] != roomID || values[3] != "1" || values[1] == nil || values[2] == nil {
		return ProjectedLike{}, redis.Nil
	}
	count, ok := values[1].(string)
	if !ok {
		return ProjectedLike{}, errors.New("点赞数类型无效")
	}
	revision, ok := values[2].(string)
	if !ok {
		return ProjectedLike{}, errors.New("投影版本类型无效")
	}
	return ProjectedLike{Count: count, Revision: revision}, nil
}

//go:embed lua/leaderboard.lua
var leaderboardLua string

//go:embed lua/project_like.lua
var projectLikeLua string

//go:embed lua/read_board.lua
var readBoardLua string

var readBoardScript = redis.NewScript(leaderboardLua + readBoardLua)

type RankedUser struct {
	UserID string
	Count  string
}

type BoardSnapshot struct {
	Revision      string
	LikesRevision string
	Personal      string
	Top           []RankedUser
}

func (repository *ProjectionRepository) sessionKey(sessionID string) string {
	return "ls:v1:projection:" + repository.epoch + ":session:" + sessionID
}

// ReadBoard 读取同一版本的公共榜单与个人数；已知版本只需返回个人部分。
func (repository *ProjectionRepository) ReadBoard(ctx context.Context, roomID, sessionID, userID, knownVersion string) (BoardSnapshot, error) {
	key := repository.sessionKey(sessionID)
	values, err := readBoardScript.Run(ctx, repository.client, []string{key, key + ":users", key + ":top"}, roomID, userID, knownVersion).StringSlice()
	if err != nil {
		return BoardSnapshot{}, fmt.Errorf("读取本场点赞榜: %w", err)
	}
	if len(values) < 3 || (len(values)-3)%2 != 0 {
		return BoardSnapshot{}, errors.New("点赞榜快照格式无效")
	}
	snapshot := BoardSnapshot{Revision: values[0], LikesRevision: values[1], Personal: values[2]}
	for i := 3; i < len(values); i += 2 {
		snapshot.Top = append(snapshot.Top, RankedUser{UserID: values[i], Count: values[i+1]})
	}
	return snapshot, nil
}

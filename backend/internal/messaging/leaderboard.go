package messaging

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

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

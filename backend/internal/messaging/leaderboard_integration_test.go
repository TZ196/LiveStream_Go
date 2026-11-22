package messaging

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 同分、掉榜、重复消费和超过浮点精度的分数均经过真实 Redis 脚本。
func TestLeaderboardIntegration(t *testing.T) {
	addr := os.Getenv("LIVESTREAM_TEST_REDIS")
	if addr == "" {
		t.Skip("未指定隔离 Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	repo, err := NewProjectionRepository(client, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, session := range []string{"1", "2", "3"} {
		if err := repo.InitializeNewSession(ctx, "8", session); err != nil {
			t.Fatal(err)
		}
	}
	makeRecord := func(user, session string, count int64) *kgo.Record {
		t.Helper()
		id, err := uuid.NewV7()
		if err != nil {
			t.Fatal(err)
		}
		event, err := NewLikeBatch(user, "8", session, id.String(), count, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		key, value, err := event.KafkaRecord()
		if err != nil {
			t.Fatal(err)
		}
		return &kgo.Record{Topic: "live.events", Key: key, Value: value}
	}
	apply := func(record *kgo.Record) ProjectedLike {
		t.Helper()
		value, err := repo.ApplyLike(ctx, record)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	read := func(session, user string) BoardSnapshot {
		t.Helper()
		snap, err := repo.ReadBoard(ctx, "8", session, user, "")
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}
	for id := 101; id >= 1; id-- {
		apply(makeRecord(strconv.Itoa(id), "1", 5))
	}
	first := read("1", "101")
	if len(first.Top) != 100 || first.Personal != "5" {
		t.Fatal(first)
	}
	for i, row := range first.Top {
		if row.UserID != strconv.Itoa(i+1) || row.Count != "5" {
			t.Fatalf("同分排序错误 %d: %+v", i, row)
		}
	}
	record := makeRecord("101", "1", 1)
	entered := apply(record)
	replayed := apply(record)
	if !replayed.Duplicate || entered.Personal != "6" || replayed.Personal != "6" || entered.Revision != replayed.Revision {
		t.Fatal(entered, replayed)
	}
	dropped := read("1", "100")
	if dropped.Top[0].UserID != "101" || dropped.Top[99].UserID != "99" || dropped.Personal != "5" {
		t.Fatal(dropped)
	}
	outside := apply(makeRecord("102", "1", 2))
	if outside.BoardRevision != dropped.Revision || outside.Personal != "2" {
		t.Fatal(outside)
	}
	if none := read("1", "999"); none.Personal != "0" {
		t.Fatal(none)
	}
	if empty := read("2", "101"); len(empty.Top) != 0 || empty.Personal != "0" {
		t.Fatal(empty)
	}
	apply(makeRecord("2", "3", 9007199254740992))
	apply(makeRecord("18446744073709551615", "3", 9007199254740993))
	big := read("3", "2")
	if big.Top[0].UserID != "18446744073709551615" || big.Top[0].Count != "9007199254740993" {
		t.Fatal(big.Top)
	}
	// 已有参与者的 Hash 丢失时应返回不可用，不能把榜外用户当作零。
	if err := client.Del(ctx, repo.sessionKey("1")+":users").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadBoard(ctx, "8", "1", "101", ""); err == nil {
		t.Fatal("缺失的用户计数未被检测")
	}
	if _, err := repo.ApplyLike(ctx, makeRecord("101", "1", 4)); err == nil {
		t.Fatal("不完整投影仍接受计数")
	}
}

// 暂存汇总过期或与原总数不符时，旧榜与总数都应原样保留。
func TestLeaderboardInstallGuard(t *testing.T) {
	addr := os.Getenv("LIVESTREAM_TEST_REDIS")
	if addr == "" {
		t.Skip("未指定隔离 Redis")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	repo, err := NewProjectionRepository(client, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.InitializeNewSession(ctx, "1", "1"); err != nil {
		t.Fatal(err)
	}
	key := repo.sessionKey("1")
	staged := []string{key + ":tmp:users", key + ":tmp:top"}
	keys := []string{key, key + ":users", key + ":top", staged[0], staged[1]}
	if err = stageBoard.Run(ctx, client, staged, "9", "5").Err(); err != nil {
		t.Fatal(err)
	}
	if err = installBoard.Run(ctx, client, keys, "1", 5, 0, 1).Err(); err == nil {
		t.Fatal("错误安装与房间总数不符的榜单")
	}
	if err = installBoard.Run(ctx, client, keys, "1", 0, 0, 2).Err(); err == nil {
		t.Fatal("错误安装缺少条目的榜单")
	}
	snap, err := repo.ReadBoard(ctx, "1", "1", "9", "")
	if err != nil || len(snap.Top) != 0 || snap.Personal != "0" {
		t.Fatal(snap, err)
	}
}

// 在双消费组用例的隔离场次中还原旧版 Redis 结构，保留真实 Kafka 位移和 MySQL 唯一批次。
func verifyLeaderboardUpgrade(t *testing.T, initializer *ProjectionInitializer, broker, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	repo := initializer.projection
	key := repo.sessionKey(id)
	if err := repo.client.HDel(ctx, key, "board_revision", "board_through", "board_users").Err(); err != nil {
		t.Fatal(err)
	}
	if err := repo.client.Del(ctx, key+":users", key+":top").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReadBoard(ctx, id, id, id, ""); err == nil {
		t.Fatal("旧结构不应返回空榜")
	}
	for range 2 {
		if err := initializer.BackfillLeaderboards(ctx, []string{broker}); err != nil {
			t.Fatal(err)
		}
		board, err := repo.ReadBoard(ctx, id, id, id, "")
		if err != nil || board.Personal != "38" || board.LikesRevision != "38" || len(board.Top) != 1 || board.Top[0] != (RankedUser{UserID: id, Count: "38"}) {
			t.Fatal(board, err)
		}
	}
}

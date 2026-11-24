package likes

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestProjectionIntegration 验证 Redis 中重复事件原子去重、冲突拒绝及检查点不回退。
func TestProjectionIntegration(t *testing.T) {
	address := os.Getenv("LIVESTREAM_TEST_REDIS")
	if address == "" {
		t.Skip("未指定隔离 Redis")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	epoch := uuid.NewString()
	repo, err := NewProjectionRepository(client, epoch)
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewLikeEvent("1", "2", "3", id.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	raw := &kgo.Record{Topic: "live.events", Key: key, Value: value, Partition: 0, Offset: 0}
	record, err := DecodeLikeRecord(raw, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ApplyLike(ctx, record); err == nil {
		t.Fatal("未初始化的投影应拒绝")
	}
	if err := repo.InitializeNewSession(ctx, "2", "3"); err != nil {
		t.Fatal(err)
	}
	first, err := repo.ApplyLike(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	record.NextOffset = 2
	second, err := repo.ApplyLike(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if first.Count != "1" || first.Duplicate || second.Count != "1" || !second.Duplicate {
		t.Fatalf("重复处理结果不正确: %+v %+v", first, second)
	}
	record.NextOffset = 1
	if _, err := repo.ApplyLike(ctx, record); err != nil {
		t.Fatal(err)
	}
	offset, err := client.Get(ctx, "ls:v1:projection:"+epoch+":partition:0").Result()
	if err != nil || offset != "2" {
		t.Fatalf("检查点回退: %s %v", offset, err)
	}
	conflict, err := NewLikeEvent("1", "2", "4", id.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.InitializeNewSession(ctx, "2", "4"); err != nil {
		t.Fatal(err)
	}
	raw.Key, raw.Value, err = conflict.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	raw.Offset = 2
	record, err = DecodeLikeRecord(raw, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ApplyLike(ctx, record); err == nil {
		t.Fatal("不同内容使用同一 event_id 应被拒绝")
	}
}

// TestLeaderboardProjectionIntegration 覆盖前百截断、榜外个人数、同分次序及整数溢出的原子拒绝。
func TestLeaderboardProjectionIntegration(t *testing.T) {
	address := os.Getenv("LIVESTREAM_TEST_REDIS")
	if address == "" {
		t.Skip("需要隔离 Redis")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	repo, err := NewProjectionRepository(client, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.InitializeNewSession(ctx, "2", "3"); err != nil {
		t.Fatal(err)
	}
	for userID := int64(1); userID <= 106; userID++ {
		count := min(userID, 105)
		event, err := NewLikeBatch(strconv.FormatInt(userID, 10), "2", "3", uuid.Must(uuid.NewV7()).String(), count, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		key, value, err := event.KafkaRecord()
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodeLikeRecord(&kgo.Record{Topic: "live.events", Key: key, Value: value, Offset: userID - 1}, "test-topic")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.ApplyLike(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	board, err := repo.ReadBoard(ctx, "2", "3", "1", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(board.Top) != 100 || board.Top[0].UserID != "105" || board.Top[1].UserID != "106" || board.Top[99].UserID != "7" || board.Personal != "1" || board.LikesRevision != "5670" {
		t.Fatalf("前百榜或榜外个人数不正确: %+v", board)
	}
	known, err := repo.ReadBoard(ctx, "2", "3", "1", board.Revision)
	if err != nil || len(known.Top) != 0 || known.Personal != "1" {
		t.Fatal("相同版本查询未保留个人数或重复返回公共榜")
	}
	event, err := NewLikeBatch("1", "2", "3", uuid.Must(uuid.NewV7()).String(), 9223372036854775807, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeLikeRecord(&kgo.Record{Topic: "live.events", Key: key, Value: value, Offset: 106}, "test-topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ApplyLike(ctx, record); err == nil {
		t.Fatal("溢出批次未拒绝")
	}
	after, err := repo.ReadBoard(ctx, "2", "3", "1", "")
	if err != nil || !reflect.DeepEqual(board, after) {
		t.Fatal("溢出后榜单或计数发生部分写入")
	}
	offset, err := client.Get(ctx, "ls:v1:projection:"+repo.epoch+":partition:0").Result()
	if err != nil || offset != "106" {
		t.Fatal("溢出后 Redis 分区位置被推进")
	}
}

package messaging

import (
	"context"
	"os"
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
	event, err := NewLikeBatch("1", "2", "3", id.String(), 100000, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "live.events", Key: key, Value: value, Partition: 0, Offset: 0}
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
	record.Offset = 1
	second, err := repo.ApplyLike(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if first.Count != "100000" || first.Revision != "100000" || first.Duplicate || second.Count != "100000" || !second.Duplicate {
		t.Fatalf("重复处理结果不正确: %+v %+v", first, second)
	}
	record.Offset = 0
	if _, err := repo.ApplyLike(ctx, record); err != nil {
		t.Fatal(err)
	}
	offset, err := client.Get(ctx, "ls:v1:projection:"+epoch+":partition:0").Result()
	if err != nil || offset != "2" {
		t.Fatalf("检查点回退: %s %v", offset, err)
	}
	if err := repo.InitializeNewSession(ctx, "2", "4"); err != nil {
		t.Fatal(err)
	}
	// 同一 ID 改数量或改场次都属于内容冲突。
	for _, changed := range []struct {
		session string
		count   int64
	}{{"3", 5}, {"4", 100000}} {
		conflict, err := NewLikeBatch("1", "2", changed.session, id.String(), changed.count, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		record.Key, record.Value, err = conflict.KafkaRecord()
		if err != nil {
			t.Fatal(err)
		}
		record.Offset = 2
		if _, err := repo.ApplyLike(ctx, record); err == nil {
			t.Fatal("不同内容使用同一 event_id 应被拒绝")
		}
	}

	// revision 先到达整数上界时，likes、去重标记和分区位移都应保持原值。
	sessionKey := "ls:v1:projection:" + epoch + ":session:3"
	if err := client.HSet(ctx, sessionKey, "revision", "9223372036854775806", "board_through", "9223372036854775806").Err(); err != nil {
		t.Fatal(err)
	}
	newID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	overflow, err := NewLikeBatch("1", "2", "3", newID.String(), 2, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	record.Key, record.Value, err = overflow.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.ApplyLike(ctx, record); err == nil {
		t.Fatal("错误接受溢出批次")
	}
	snapshot, err := repo.ReadSession(ctx, "2", "3")
	if err != nil || snapshot.Count != "100000" || snapshot.Revision != "9223372036854775806" {
		t.Fatalf("溢出导致部分写入：%+v %v", snapshot, err)
	}
	position, err := client.Get(ctx, "ls:v1:projection:"+epoch+":partition:0").Result()
	if err != nil || position != "2" {
		t.Fatalf("失败后位移变化：%s %v", position, err)
	}
	seen, err := client.Exists(ctx, "ls:v1:projection:"+epoch+":processed:"+overflow.EventID).Result()
	if err != nil || seen != 0 {
		t.Fatalf("失败后写入去重记录：%d %v", seen, err)
	}
}

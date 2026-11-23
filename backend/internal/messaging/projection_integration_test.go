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
	event, err := NewLikeEvent("1", "2", "3", id.String(), time.Now())
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
	if first.Count != "1" || first.Duplicate || second.Count != "1" || !second.Duplicate {
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
	conflict, err := NewLikeEvent("1", "2", "4", id.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.InitializeNewSession(ctx, "2", "4"); err != nil {
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

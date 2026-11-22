package messaging

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestDualConsumerIntegration 仅在全新隔离 Kafka、MySQL 和 Redis 环境验证双消费组。
func TestDualConsumerIntegration(t *testing.T) {
	broker, dsn, address := os.Getenv("LIVESTREAM_TEST_KAFKA"), os.Getenv("LIVESTREAM_TEST_MYSQL_DSN"), os.Getenv("LIVESTREAM_TEST_REDIS")
	if broker == "" || dsn == "" || address == "" {
		t.Skip("需要三个隔离测试服务")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	n := uint64(time.Now().UnixNano()%1000000000 + 2000000000)
	id := fmt.Sprint(n)
	if err = db.Exec("INSERT INTO users (id, username, password_hash, created_at) VALUES (?, ?, ?, ?)", n, "t"+id, "test", now).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Exec("INSERT INTO live_rooms (id, owner_id, title, status, created_at) VALUES (?, ?, ?, 'OPEN', ?)", n, n, "test", now).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Exec("INSERT INTO live_sessions (id, room_id, started_at) VALUES (?, ?, ?)", n, n, now).Error; err != nil {
		t.Fatal(err)
	}
	epoch := uuid.NewString()
	projection, err := NewProjectionRepository(client, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if err = projection.InitializeNewSession(ctx, id, id); err != nil {
		t.Fatal(err)
	}
	persistence := NewPersistenceRepository(db)
	realtime, err := NewConsumer(ctx, []string{broker}, "live-realtime")
	if err != nil {
		t.Fatal(err)
	}
	defer realtime.Close()
	worker, err := NewConsumer(ctx, []string{broker}, "live-persistence")
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	results := make(chan error, 2)
	var realtimeDone, persistenceDone atomic.Int32
	go func() {
		results <- realtime.Run(ctx, func(ctx context.Context, record *kgo.Record, _ string) error {
			_, err := projection.ApplyLike(ctx, record)
			if err == nil {
				realtimeDone.Add(1)
			}
			return err
		})
	}()
	go func() {
		results <- worker.Run(ctx, func(ctx context.Context, record *kgo.Record, topicID string) error {
			err := persistence.PersistLike(ctx, record, topicID)
			if err == nil {
				persistenceDone.Add(1)
			}
			return err
		})
	}()
	producer, err := NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	request, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewLikeEvent(id, id, id, request.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = producer.PublishLike(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-results:
			t.Fatalf("消费者提前停止: %v", err)
		case <-ctx.Done():
			t.Fatal("双消费组未在时间内完成")
		case <-ticker.C:
			likes, redisErr := client.HGet(ctx, "ls:v1:projection:"+epoch+":session:"+id, "likes").Result()
			var count int64
			dbErr := db.Table("interaction_events").Where("event_id = ?", event.EventID).Count(&count).Error
			if redisErr == nil && dbErr == nil && likes == "1" && count == 1 && realtimeDone.Load() >= 2 && persistenceDone.Load() >= 2 {
				cancel()
				for i := 0; i < 2; i++ {
					if err := <-results; err != nil && ctx.Err() == nil {
						t.Fatal(err)
					}
				}
				return
			}
		}
	}
}

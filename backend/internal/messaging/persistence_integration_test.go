package messaging

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// TestPersistenceIntegration 只在显式提供的隔离测试库中验证重复写入和事务位移。
func TestPersistenceIntegration(t *testing.T) {
	dsn := os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未指定隔离 MySQL 测试库")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	userID := uint64(now.UnixNano()%1000000000 + 1000000000)
	roomID := userID
	sessionID := userID
	username := fmt.Sprintf("t%d", userID)
	if err := db.Exec("INSERT INTO users (id, username, password_hash, created_at) VALUES (?, ?, ?, ?)", userID, username, "test", now).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO live_rooms (id, owner_id, title, status, created_at) VALUES (?, ?, ?, 'OPEN', ?)", roomID, userID, "test", now).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO live_sessions (id, room_id, started_at) VALUES (?, ?, ?)", sessionID, roomID, now).Error; err != nil {
		t.Fatal(err)
	}
	requestID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewLikeEvent(fmt.Sprint(userID), fmt.Sprint(roomID), fmt.Sprint(sessionID), requestID.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "live.events", Partition: 0, Offset: 0, Key: key, Value: value}
	repo := NewPersistenceRepository(db)
	if err := repo.PersistLike(context.Background(), record, "isolated-test-topic"); err != nil {
		t.Fatal(err)
	}
	record.Offset++
	if err := repo.PersistLike(context.Background(), record, "isolated-test-topic"); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Table("interaction_events").Where("event_id = ?", event.EventID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("点赞事件数量 = %d, error = %v", count, err)
	}
	var offset int64
	if err := db.Table("worker_checkpoints").Where("consumer_group = ? AND topic_id = ? AND partition_id = ?", "live-persistence", "isolated-test-topic", 0).Pluck("next_offset", &offset).Error; err != nil || offset != 2 {
		t.Fatalf("检查点 = %d, error = %v", offset, err)
	}
	record.Offset++
	record.Key = []byte("wrong")
	if err := repo.PersistLike(context.Background(), record, "isolated-test-topic"); err == nil {
		t.Fatal("非法分区键未拒绝")
	}
	if err := db.Table("worker_checkpoints").Where("consumer_group = ? AND topic_id = ? AND partition_id = ?", "live-persistence", "isolated-test-topic", 0).Pluck("next_offset", &offset).Error; err != nil || offset != 2 {
		t.Fatalf("失败后检查点被推进: %d, %v", offset, err)
	}
}

package danmaku

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/TZ196/LiveStream_Go/backend/migrations"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type confirmedThenUnknown struct {
	producer  *Producer
	mu        sync.Mutex
	lose      bool
	published int
}

func (publisher *confirmedThenUnknown) Publish(ctx context.Context, event Event) error {
	err := publisher.producer.Publish(ctx, event)
	if err != nil {
		return err
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publisher.published++
	if publisher.lose {
		publisher.lose = false
		return errors.New("模拟 Broker 已确认但应用未获结果")
	}
	return nil
}
func (publisher *confirmedThenUnknown) count() int {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return publisher.published
}

func waitFor(t *testing.T, ctx context.Context, description string, check func() bool) {
	t.Helper()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatal("等待超时: " + description)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestDanmakuIntegration 仅允许显式指定的空测试库和独立 Kafka；Redis 使用测试实例 DB 15。
func TestDanmakuIntegration(t *testing.T) {
	if os.Getenv("LIVESTREAM_TEST_DANMAKU") != "1" {
		t.Skip("需要显式启用隔离弹幕测试")
	}
	dsn, broker, address := os.Getenv("LIVESTREAM_TEST_MYSQL_DSN"), os.Getenv("LIVESTREAM_TEST_KAFKA"), os.Getenv("LIVESTREAM_TEST_REDIS")
	if dsn == "" || broker == "" || address == "" {
		t.Fatal("缺少隔离中间件地址")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("无法连接隔离 MySQL")
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := migrations.Run(ctx, db, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	repository := NewRepository(db)
	exists, err := repository.HasHistory(ctx)
	if err != nil || exists {
		t.Fatal("弹幕集成要求空业务库")
	}
	client := redis.NewClient(&redis.Options{Addr: address, DB: 15})
	defer client.Close()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	store := NewRequestStore(client)
	if err := store.Check(ctx, false); err != nil {
		t.Fatal(err)
	}
	users := user.NewRepository(db)
	if err := db.Exec("INSERT INTO users (id,username,password_hash) VALUES (1001,'danmaku_a','integration-placeholder'),(1002,'danmaku_b','integration-placeholder')").Error; err != nil {
		t.Fatal(err)
	}
	rooms := room.NewService(room.NewRepository(db))
	live, err := rooms.Create(ctx, 1001, "弹幕隔离集成")
	if err != nil {
		t.Fatal(err)
	}
	session, err := rooms.ActiveSession(ctx, live.ID)
	if err != nil {
		t.Fatal(err)
	}
	roomID, sessionID := strconv.FormatUint(live.ID, 10), strconv.FormatUint(session.ID, 10)
	moderator, err := LoadModerator("../../configs/moderation_words.tsv")
	if err != nil {
		t.Fatal(err)
	}
	producer, err := NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	publisher := &confirmedThenUnknown{producer: producer}
	service := NewService(store, repository, moderator, publisher, rooms, users)
	service.SetHealthy(true)
	service.SetConsuming(true)
	realtime, err := OpenConsumerGroup(ctx, []string{broker}, RealtimeGroup, 4, store.ValidateRealtime)
	if err != nil {
		t.Fatal(err)
	}
	persistent, err := OpenConsumerGroup(ctx, []string{broker}, PersistenceGroup, 4, repository.ValidateCheckpoint)
	if err != nil {
		realtime.Close()
		t.Fatal(err)
	}
	consuming, stop := context.WithCancel(ctx)
	var loops sync.WaitGroup
	var mu sync.Mutex
	received := make(map[string]int)
	records := make(map[string]Record)
	causes := make(chan error, 2)
	loops.Add(2)
	go func() {
		defer loops.Done()
		causes <- realtime.Run(consuming, func(ctx context.Context, record Record) error {
			mu.Lock()
			received[record.Event.EventID]++
			mu.Unlock()
			return store.MarkRealtime(ctx, record)
		})
	}()
	go func() {
		defer loops.Done()
		causes <- persistent.Run(consuming, func(ctx context.Context, record Record) error {
			err := repository.Persist(ctx, record)
			if err == nil {
				mu.Lock()
				records[record.Event.EventID] = record
				mu.Unlock()
			}
			return err
		})
	}()
	defer func() {
		stop()
		loops.Wait()
		realtime.Close()
		persistent.Close()
		close(causes)
		for err := range causes {
			if err != nil {
				t.Errorf("消费循环异常: %v", err)
			}
		}
	}()
	submit := func(userID, request, text string) (Event, error) {
		return service.Submit(ctx, userID, roomID, sessionID, request, text)
	}
	request := uuid.Must(uuid.NewV7()).String()
	var first Event
	t.Run("concurrent_request_once", func(t *testing.T) {
		var clients sync.WaitGroup
		for i := 0; i < 16; i++ {
			clients.Add(1)
			go func() {
				defer clients.Done()
				event, err := submit("1001", request, "并发原请求")
				if err == nil {
					mu.Lock()
					first = event
					mu.Unlock()
					return
				}
				fault := AsFault(err)
				if !fault.Pending {
					t.Errorf("并发请求被错误拒绝: %v", err)
				}
			}()
		}
		clients.Wait()
		if first.EventID == "" {
			t.Fatal("原请求未接受")
		}
		if publisher.count() != 1 {
			t.Fatalf("正常并发发布次数=%d", publisher.count())
		}
		before := publisher.count()
		event, err := submit("1001", request, "并发原请求")
		if err != nil || event != first || publisher.count() != before {
			t.Fatal("已接受重试没有返回原事件")
		}
		if _, err := submit("1001", request, "修改后的内容"); AsFault(err).Code != "REQUEST_CONFLICT" {
			t.Fatal("变更内容未返回冲突")
		}
	})
	t.Run("rejection_and_quota_once", func(t *testing.T) {
		id := uuid.Must(uuid.NewV7()).String()
		before := publisher.count()
		for i := 0; i < 3; i++ {
			if _, err := submit("1001", id, "刷单垫付"); AsFault(err).Code != "CONTENT_BLOCKED" {
				t.Fatalf("词库未拒绝: %v", err)
			}
		}
		quota, err := client.ZCard(ctx, "danmaku:rate:1001").Result()
		if err != nil || quota != 2 || publisher.count() != before {
			t.Fatalf("拒绝重试重复限流或进入 Kafka: quota=%d", quota)
		}
	})
	var unknown Event
	t.Run("unknown_publish_freezes_review_version", func(t *testing.T) {
		id := uuid.Must(uuid.NewV7()).String()
		publisher.mu.Lock()
		publisher.lose = true
		publisher.mu.Unlock()
		if _, err := submit("1001", id, "冻结版本测试"); !AsFault(err).Pending {
			t.Fatalf("未确认发布被伪报: %v", err)
		}
		nextModerator, err := ReadModerator(strings.NewReader(testDictionary("冻结版本测试")))
		if err != nil {
			t.Fatal(err)
		}
		changed := NewService(store, repository, nextModerator, publisher, rooms, users)
		changed.SetHealthy(true)
		changed.SetConsuming(true)
		unknown, err = changed.Submit(ctx, "1001", roomID, sessionID, id, "冻结版本测试")
		if err != nil || unknown.ModerationVersion != moderator.Version {
			t.Fatalf("重试被新词库重新审核: %v", err)
		}
		count, _ := client.ZCard(ctx, "danmaku:rate:1001").Result()
		if count != 3 {
			t.Fatalf("未知结果重试重复扣额: %d", count)
		}
	})
	t.Run("sliding_window_and_original_retry", func(t *testing.T) {
		var last Event
		for i := 0; i < 10; i++ {
			event, err := submit("1002", uuid.Must(uuid.NewV7()).String(), fmt.Sprintf("顺序弹幕 %d", i))
			if err != nil {
				t.Fatal(err)
			}
			last = event
		}
		if _, err := submit("1002", uuid.Must(uuid.NewV7()).String(), "第十一条"); AsFault(err).Code != "RATE_LIMITED" {
			t.Fatal("滑动窗口未限流")
		}
		if event, err := submit("1002", last.RequestID, last.Payload.Content); err != nil || event != last {
			t.Fatalf("已接受原请求被频控拒绝: %v", err)
		}
	})
	waitFor(t, ctx, "12 个唯一事件持久化", func() bool {
		select {
		case err := <-causes:
			t.Fatalf("消费提前失败: %v", err)
		default:
		}

		var count int64
		db.Model(&Message{}).Count(&count)
		mu.Lock()
		realtimeCount := len(received)
		mu.Unlock()
		return count == 12 && realtimeCount == 12
	})
	t.Run("both_groups_and_persistence_idempotence", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()
		if len(records) != 12 || len(received) != 12 {
			t.Fatalf("双组未各自处理全部业务事件: SQL=%d RT=%d", len(records), len(received))
		}
		if received[unknown.EventID] < 1 {
			t.Fatal("未确认事件没有进入实时组")
		}
	})
	t.Run("duplicate_conflict_and_transaction_rollback", func(t *testing.T) {
		mu.Lock()
		record := records[first.EventID]
		mu.Unlock()
		if err := repository.Persist(ctx, record); err != nil {
			t.Fatal(err)
		}
		var before, after int64
		db.Table("worker_checkpoints").Where("consumer_group=? AND partition_id=?", PersistenceGroup, record.Partition).Select("next_offset").Scan(&before)
		altered := record
		altered.Event.Payload.Content = "指纹冲突"
		altered.NextOffset += 1000
		if err := repository.Persist(ctx, altered); !errors.Is(err, ErrConflict) {
			t.Fatalf("冲突没有终止事务: %v", err)
		}
		invalid, _ := NewRequest("999999", "1", "1", uuid.Must(uuid.NewV7()).String(), "不存在的用户", time.Now())
		invalid.Payload.Username = "ghost"
		invalid.ModerationVersion = moderator.Version
		altered.Event = invalid
		if err := repository.Persist(ctx, altered); err == nil {
			t.Fatal("外键失败没有回滚")
		}
		db.Table("worker_checkpoints").Where("consumer_group=? AND partition_id=?", PersistenceGroup, record.Partition).Select("next_offset").Scan(&after)
		if before != after {
			t.Fatal("失败事务推进了检查点")
		}
	})
	t.Run("history_cursor_and_scope", func(t *testing.T) {
		page, err := repository.History(ctx, live.ID, session.ID, 2, "")
		if err != nil || len(page.Items) != 2 || page.NextCursor == nil {
			t.Fatalf("历史分页错误: %v", err)
		}
		second, err := repository.History(ctx, live.ID, session.ID, 2, *page.NextCursor)
		if err != nil || second.Items[0].ID >= page.Items[1].ID {
			t.Fatal("历史游标没有按 ID 倒序")
		}
		if _, err := repository.History(ctx, live.ID+1, session.ID, 2, *page.NextCursor); AsFault(err).Code != "INVALID_CURSOR" {
			t.Fatal("游标未绑定房间")
		}
	})
	t.Run("lost_request_cache_can_use_persisted_proof", func(t *testing.T) {
		before := publisher.count()
		if err := client.Del(ctx, requestKey("1001", first.RequestID)).Err(); err != nil {
			t.Fatal(err)
		}
		event, err := submit("1001", first.RequestID, first.Payload.Content)
		if err != nil || event != first || publisher.count() != before {
			t.Fatalf("持久化证据未恢复接受结果: %v", err)
		}
	})
	t.Run("lost_ledger_does_not_reinvent_unknown_event", func(t *testing.T) {
		pendingID := uuid.Must(uuid.NewV7()).String()
		candidate, _ := NewRequest("1001", roomID, sessionID, pendingID, "尚无持久化证据", time.Now())
		claim, err := store.Begin(ctx, candidate, "test-executor")
		if err != nil || claim[0] != "OWNED" {
			t.Fatal("测试请求未登记")
		}
		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		if err := store.Check(ctx, true); err != nil {
			t.Fatal(err)
		}
		before := publisher.count()
		if _, err := submit("1001", pendingID, candidate.Payload.Content); !AsFault(err).Pending {
			t.Fatal("丢失状态被当作新请求")
		}
		if publisher.count() != before {
			t.Fatal("找不到冻结事件仍然自动发布")
		}
		if _, err := submit("1001", first.RequestID, first.Payload.Content); err != nil {
			t.Fatal("Redis 丢失后不能读取可靠历史")
		}
	})
}

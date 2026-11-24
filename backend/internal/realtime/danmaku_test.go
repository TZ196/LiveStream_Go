package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/google/uuid"
)

// TestDanmakuDispatchOrder 验证并行分区通过同场次分发后，两条连接看到相同顺序。
func TestDanmakuDispatchOrder(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "")
	t.Cleanup(func() { manager.dispatchCancel(); manager.dispatchWait.Wait() })
	first, second := manager.newConnection(nil), manager.newConnection(nil)
	manager.register(1, 2, "3", first)
	manager.register(1, 2, "4", second)
	other := manager.newConnection(nil)
	manager.register(5, 6, "3", other)
	var workers sync.WaitGroup
	failures := make(chan error, 2)
	for userID := 3; userID <= 4; userID++ {
		workers.Add(1)
		go func(userID int) {
			defer workers.Done()
			for index := 0; index < 20; index++ {
				event, err := danmaku.NewRequest(fmt.Sprint(userID), "1", "2", uuid.Must(uuid.NewV7()).String(), fmt.Sprintf("%d/%d", userID, index), time.Now())
				if err == nil {
					event.Payload.Username = fmt.Sprintf("user%d", userID)
					event.ModerationVersion = strings.Repeat("0", 64)
					err = manager.QueueDanmaku(context.Background(), danmaku.Record{Event: event})
					if err == nil {
						// 消费重放不能再次进入发送队列。
						err = manager.QueueDanmaku(context.Background(), danmaku.Record{Event: event})
					}
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}(userID)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if len(first.send) != 40 || len(second.send) != 40 || len(other.send) != 0 {
		t.Fatal("弹幕重复入队、遗漏或跨房间泄漏")
	}
	seen := map[string]int{"3": 0, "4": 0}
	for index := 0; index < 40; index++ {
		left, right := <-first.send, <-second.send
		if string(left.payload) != string(right.payload) {
			t.Fatal("同场次两个客户端的分发顺序不同")
		}
		var frame struct {
			Data struct {
				UserID  string `json:"user_id"`
				Content string `json:"content"`
			}
		}
		if err := json.Unmarshal(left.payload, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Data.Content != fmt.Sprintf("%s/%d", frame.Data.UserID, seen[frame.Data.UserID]) {
			t.Fatal("同用户的分区消费顺序被改变")
		}
		seen[frame.Data.UserID]++
	}
}

// TestDanmakuUnaffectedByLikePause 验证点赞暂停和慢连接不会阻塞弹幕健康连接。
func TestDanmakuUnaffectedByLikePause(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "")
	t.Cleanup(func() { manager.dispatchCancel(); manager.dispatchWait.Wait() })
	slow, healthy := manager.newConnection(nil), manager.newConnection(nil)
	manager.register(1, 2, "3", slow)
	manager.register(1, 2, "4", healthy)
	for index := 0; index < cap(slow.send); index++ {
		slow.enqueue([]byte("pending"))
	}
	manager.Pause("RECOVERING")
	select {
	case <-healthy.done:
		t.Fatal("点赞恢复关闭了共享连接")
	default:
	}
	event, err := danmaku.NewRequest("3", "1", "2", uuid.Must(uuid.NewV7()).String(), "正常弹幕", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.QueueDanmaku(context.Background(), danmaku.Record{Event: event}); err != nil {
		t.Fatal(err)
	}
	if len(healthy.send) != 1 {
		t.Fatal("点赞暂停或慢连接阻塞了弹幕")
	}
	select {
	case <-slow.done:
	default:
		t.Fatal("慢连接满队列后没有逻辑关闭")
	}
}

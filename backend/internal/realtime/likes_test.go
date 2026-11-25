package realtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
)

// TestLikeReplayAfterLockTimeout 覆盖 Redis 已计数但内存同步超时后的重复投递。
func TestLikeReplayAfterLockTimeout(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "test")
	t.Cleanup(func() { manager.dispatchCancel(); manager.dispatchWait.Wait() })
	manager.SetAvailable(true)
	client := manager.newConnection(nil)
	view := manager.register(1, 2, "3", client)
	event := likes.LikeEvent{RoomID: "1", SessionID: "2", UserID: "3"}
	result := likes.ProjectedLike{Count: "5", Revision: "5", Personal: "5", BoardRevision: "5"}
	view.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := manager.QueueLikeUpdate(ctx, event, result)
	cancel()
	view.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等锁应超时: %v", err)
	}
	result.Duplicate = true
	if err := manager.QueueLikeUpdate(context.Background(), event, result); err != nil {
		t.Fatal(err)
	}
	if view.pending == nil || view.pending.Count != "5" || view.people["3"].count != "5" || view.wantedBoard != 5 {
		t.Fatal("重放未补齐场次、个人和榜单快照")
	}
	manager.flushLikeUpdates()
	if len(client.send) != 1 {
		t.Fatal("重放结果没有推送")
	}
	if err := manager.QueueLikeUpdate(context.Background(), event, result); err != nil {
		t.Fatal(err)
	}
	manager.flushLikeUpdates()
	if len(client.send) != 1 {
		t.Fatal("同版本重复快照再次推送")
	}
}

// TestSlowConnectionIsolation 验证满队列先逻辑关闭，其余连接与其他忙场次独立分发。
func TestSlowConnectionIsolation(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "test")
	t.Cleanup(func() { manager.dispatchCancel(); manager.dispatchWait.Wait() })
	manager.SetAvailable(true)
	slow := manager.newConnection(nil)
	healthy := manager.newConnection(nil)
	view := manager.register(1, 2, "3", slow)
	manager.register(1, 2, "4", healthy)
	for i := 0; i < cap(slow.send); i++ {
		slow.enqueue([]byte("pending"))
	}
	msg := &outbound{payload: []byte("new")}
	count, _ := manager.sendRoomFrame(view, []*connection{slow, healthy}, msg, true)
	if count != 1 || len(healthy.send) != 1 {
		t.Fatal("慢连接影响了健康连接")
	}
	select {
	case <-slow.done:
	default:
		t.Fatal("满队列连接未逻辑关闭")
	}
	busy := manager.register(5, 6, "7", manager.newConnection(nil))
	busy.pending = &likeUpdate{Count: "1"}
	view.pending = &likeUpdate{Count: "2"}
	busy.mu.Lock()
	manager.flushLikeUpdates()
	busy.mu.Unlock()
	if busy.pending == nil || view.pending != nil || len(healthy.send) != 2 {
		t.Fatal("忙场次阻止了其他场次分发，或丢失了待发快照")
	}
	view.closed.Store(true)
	fresh := manager.register(1, 2, "4", healthy)
	manager.remove(2, view)
	if fresh == view || manager.lookup(2) != fresh {
		t.Fatal("旧对象的延迟清理删除了新场次对象")
	}
}

// TestPauseWaitsForExistingOperations 验证暂停等待旧操作，但不在等点赞时占住房间准入锁。
func TestPauseWaitsForExistingOperations(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "test")
	manager.SetAvailable(true)
	manager.admission.RLock()
	manager.likeAdmission.RLock()
	roomHeld, likeHeld := true, true
	done := make(chan struct{})
	go func() {
		manager.Pause("RECOVERING")
		close(done)
	}()
	t.Cleanup(func() {
		if roomHeld {
			manager.admission.RUnlock()
		}
		if likeHeld {
			manager.likeAdmission.RUnlock()
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("暂停协程没有退出")
		}
		manager.dispatchCancel()
		manager.dispatchWait.Wait()
	})
	waitFor := func(check func() bool, message string) {
		deadline := time.Now().Add(2 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal(message)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// 写锁开始等待后，新读者不能进入；借此确认暂停确实等到了旧房间操作。
	waitFor(func() bool {
		if manager.admission.TryRLock() {
			manager.admission.RUnlock()
			return false
		}
		return true
	}, "暂停没有等待在途房间操作")
	manager.admission.RUnlock()
	roomHeld = false
	// 旧点赞仍在执行时，共享房间锁应已经释放，不能被点赞发布一起拖住。
	waitFor(func() bool {
		if !manager.admission.TryLock() {
			return false
		}
		manager.admission.Unlock()
		return true
	}, "等待在途点赞时仍占用共享房间锁")
	select {
	case <-done:
		t.Fatal("在途点赞尚未结束，暂停却提前返回")
	default:
	}
	manager.likeAdmission.RUnlock()
	likeHeld = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("在途操作结束后暂停没有返回")
	}
	if manager.Available() || manager.State() != "RECOVERING" {
		t.Fatal("暂停状态未保留")
	}
}

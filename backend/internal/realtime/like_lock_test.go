package realtime

import (
	"bytes"
	"context"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	probeHold     = 20 * time.Second
	probeDeadline = 10 * time.Second
)

// 只用于对照实验。通道里的一枚令牌代表锁，等待者直接监听取消信号。
type probeContextMutex chan struct{}

func newProbeContextMutex() probeContextMutex {
	lock := make(probeContextMutex, 1)
	lock <- struct{}{}
	return lock
}

func (m probeContextMutex) lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m:
		if err := ctx.Err(); err != nil {
			m.unlock()
			return err
		}
		return nil
	}
}

func (m probeContextMutex) unlock() { m <- struct{}{} }

func enableLockProbe(t *testing.T) {
	t.Helper()
	if os.Getenv("LIVESTREAM_TEST_LIKE_LOCKS") != "1" {
		t.Skip("显式启用 20 秒锁竞争实验")
	}
}

// 对照开始与释放资源后的 goroutine 栈，避免仅凭 go test -race 推断没有泄漏。
func probeStacks() map[string]string {
	var dump bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&dump, 2)
	stacks := make(map[string]string)
	for _, block := range strings.Split(strings.TrimSpace(dump.String()), "\n\n") {
		header, _, _ := strings.Cut(block, "\n")
		fields := strings.Fields(header)
		if len(fields) >= 2 && fields[0] == "goroutine" {
			stacks[fields[1]] = block
		}
	}
	return stacks
}

func checkLockProbeExit(t *testing.T) {
	t.Helper()
	before := probeStacks()
	t.Cleanup(func() {
		limit := time.Now().Add(5 * time.Second)
		for {
			after := probeStacks()
			var remaining []string
			for id, stack := range after {
				if _, existed := before[id]; existed {
					continue
				}
				if strings.Contains(stack, "/internal/realtime.") || strings.Contains(stack, "/internal/messaging.") ||
					strings.Contains(stack, "github.com/twmb/franz-go/") || strings.Contains(stack, "github.com/redis/go-redis/") {
					remaining = append(remaining, stack)
				}
			}
			if len(remaining) == 0 {
				t.Logf("goroutine 总数 %d → %d，新增且存活的实验/Kafka/Redis 协程 0", len(before), len(after))
				return
			}
			if time.Now().After(limit) {
				t.Errorf("资源关闭后仍有相关 goroutine：\n%s", strings.Join(remaining, "\n"))
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

func TestLikeMutexDeadline(t *testing.T) {
	enableLockProbe(t)
	checkLockProbeExit(t)
	var regular sync.Mutex
	regular.Lock()
	cancellable := newProbeContextMutex()
	if err := cancellable.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), probeDeadline)
	defer cancel()
	var workers sync.WaitGroup
	var regularWait, cancellableWait time.Duration
	var regularContext, cancellableError error
	workers.Go(func() {
		regular.Lock()
		defer regular.Unlock()
		regularWait, regularContext = time.Since(start), ctx.Err()
	})
	workers.Go(func() {
		cancellableError = cancellable.lock(ctx)
		cancellableWait = time.Since(start)
		if cancellableError == nil {
			cancellable.unlock()
		}
	})
	// A 同时持有两把互相独立的锁，两个 B 从同一起点分别等待。
	<-time.After(probeHold)
	regular.Unlock()
	cancellable.unlock()
	workers.Wait()
	t.Logf("普通 Mutex 等待 %.3fs，取得锁时 context=%v", regularWait.Seconds(), regularContext)
	t.Logf("可取消 Mutex 等待 %.3fs，返回 %v", cancellableWait.Seconds(), cancellableError)
	if regularWait < probeHold || regularContext != context.DeadlineExceeded ||
		cancellableError != context.DeadlineExceeded || cancellableWait < probeDeadline || cancellableWait >= probeHold {
		t.Fatal("锁等待行为与各自的截止时间不符")
	}
}

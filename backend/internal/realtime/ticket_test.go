package realtime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/user"
)

// TestTicketSingleUse 验证同一票据并发使用时只有一个连接能消费成功。
func TestTicketSingleUse(t *testing.T) {
	tickets := NewTickets()
	encoded, _, err := tickets.Issue(user.Identity{UserID: 1, ExpiresAt: time.Now().Add(time.Minute)}, 2, 3, "http://localhost:5173")
	if err != nil {
		t.Fatal(err)
	}
	var successes atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := tickets.consume(encoded, 2, "http://localhost:5173"); err == nil {
				successes.Add(1)
			}
		}()
	}
	wait.Wait()
	if successes.Load() != 1 {
		t.Fatalf("同一票据成功使用 %d 次", successes.Load())
	}
}

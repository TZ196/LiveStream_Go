package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
)

// 每个有观众的场次拥有独立状态。roomID 不变，closed 关闭后不再复用。
// 其余状态由 mu 保护，公开榜单的查询缓存另外由 board.mu 保护。
type audience struct {
	mu            roomMutex
	closed        atomic.Bool
	roomID        uint64
	clients       map[*connection]struct{}
	revision      int64
	pending       *likeUpdate
	pauseNotified bool
	people        map[string]*personalUpdate
	dirtyPeople   map[string]*personalUpdate
	board         publicBoard
	wantedBoard   int64
	sentBoard     int64
	lastTop       []BoardRow
}

type likeUpdate struct {
	RoomID    string `json:"room_id"`
	SessionID string `json:"session_id"`
	Count     string `json:"like_count"`
	Revision  string `json:"likes_revision"`
	Epoch     string `json:"projection_epoch"`
}

// QueueLikeUpdate 使用 Redis 此刻的权威结果补齐内存；重放也能完成上次中断的同步。
func (manager *Manager) QueueLikeUpdate(ctx context.Context, event messaging.LikeEvent, projection messaging.ProjectedLike) error {
	sessionID, err := strconv.ParseUint(event.SessionID, 10, 64)
	if err != nil {
		return err
	}
	revision, err := strconv.ParseInt(projection.Revision, 10, 64)
	if err != nil {
		return err
	}
	boardVersion, err := strconv.ParseInt(projection.BoardRevision, 10, 64)
	if err != nil {
		return err
	}
	view := manager.lookup(sessionID)
	if view == nil {
		return ctx.Err()
	}
	if err := view.mu.LockContext(ctx); err != nil {
		return err
	}
	defer view.mu.Unlock()
	if view.closed.Load() {
		return nil
	}
	// 个人数与榜单各自比较版本。其他人的点赞可推进总版本，而不改变该用户的计数。
	if person := view.people[event.UserID]; person != nil && revision > person.revision {
		person.revision = revision
		if person.count != projection.Personal {
			person.count = projection.Personal
			view.dirtyPeople[event.UserID] = person
		}
	}
	if boardVersion > view.wantedBoard {
		view.wantedBoard = boardVersion
	}
	if revision > view.revision {
		view.revision = revision
		view.pending = &likeUpdate{RoomID: event.RoomID, SessionID: event.SessionID,
			Count: projection.Count, Revision: projection.Revision, Epoch: manager.epoch}
	}
	return nil
}

// RunLikeUpdates 只合并广播，Kafka 确认、Redis 计数与数据库事务各自独立完成。
func (manager *Manager) RunLikeUpdates(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if manager.available.Load() {
				manager.flushLikeUpdates()
			} else {
				// 忙场次的暂停通知在后续周期补发，独立于消费组的恢复检查。
				manager.BroadcastUnavailable()
			}
		}
	}
}

func (manager *Manager) flushLikeUpdates() {
	for _, view := range manager.roomList() {
		// 忙场次保留 pending，下一轮继续，不耽误其他场次。
		if !view.mu.TryLock() {
			continue
		}
		update := view.pending
		if view.closed.Load() || !manager.available.Load() || update == nil {
			view.mu.Unlock()
			continue
		}
		clients := copyClients(view.clients)
		view.pending = nil
		view.mu.Unlock()

		frame, _ := json.Marshal(map[string]any{"version": 1, "type": "LIKE_UPDATE", "data": update})
		manager.deliver(view, clients, frame)
	}
}

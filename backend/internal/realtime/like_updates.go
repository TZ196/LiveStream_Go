package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
)

type likeUpdate struct {
	RoomID    string `json:"room_id"`
	SessionID string `json:"session_id"`
	Count     string `json:"like_count"`
	Revision  string `json:"likes_revision"`
	Epoch     string `json:"projection_epoch"`
}

// QueueLikeUpdate 使用 Redis 此刻的权威结果补齐内存；重放也能完成上次中断的同步。
func (manager *Manager) QueueLikeUpdate(ctx context.Context, event likes.LikeEvent, projection likes.ProjectedLike) error {
	redisAt := time.Now()
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
		manager.metrics.memoryResults.WithLabelValues("no_audience").Inc()
		return ctx.Err()
	}
	if err := view.mu.LockContext(ctx); err != nil {
		manager.metrics.memoryResults.WithLabelValues("cancelled").Inc()
		return err
	}
	if view.closed.Load() {
		view.mu.Unlock()
		manager.metrics.memoryResults.WithLabelValues("closed").Inc()
		return nil
	}
	changed := false
	// 个人数与榜单各自比较版本。其他人的点赞可推进总版本，而不改变该用户的计数。
	if person := view.people[event.UserID]; person != nil && revision > person.revision {
		person.revision = revision
		changed = true
		if person.count != projection.Personal {
			person.count = projection.Personal
			view.dirtyPeople[event.UserID] = person
		}
	}
	if boardVersion > view.wantedBoard {
		view.wantedBoard = boardVersion
		changed = true
		if boardVersion > view.sentBoard && view.boardSince.Load() == nil {
			now := time.Now()
			view.boardSince.Store(&now)
		}
	}
	if revision > view.revision {
		view.revision = revision
		changed = true
		if view.pending == nil {
			now := time.Now()
			view.pendingSince.Store(&now)
		}
		view.pending = &likeUpdate{RoomID: event.RoomID, SessionID: event.SessionID,
			Count: projection.Count, Revision: projection.Revision, Epoch: manager.epoch}
	}
	view.mu.Unlock()
	if changed {
		manager.metrics.memory.Observe(time.Since(redisAt).Seconds())
		manager.metrics.memoryResults.WithLabelValues("updated").Inc()
	} else {
		manager.metrics.memoryResults.WithLabelValues("unchanged").Inc()
	}
	return nil
}

type likeFrame struct {
	Version        int         `json:"version"`
	Type           string      `json:"type"`
	Data           *likeUpdate `json:"data"`
	ServerSentAtMS int64       `json:"server_sent_at_ms,omitempty"`
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
	start := time.Now()
	defer func() { manager.metrics.flush.Observe(time.Since(start).Seconds()) }()
	for _, view := range manager.roomList() {
		// 忙场次保留 pending，下一轮继续，不耽误其他场次。
		if !view.mu.TryLock() {
			manager.metrics.skips.WithLabelValues("likes_room_busy").Inc()
			continue
		}
		update := view.pending
		if view.closed.Load() || !manager.available.Load() || update == nil {
			view.mu.Unlock()
			continue
		}
		clients := copyClients(view.clients)
		since := view.pendingSince.Load()
		view.sendingSince.Store(since)
		view.pendingSince.Store(nil)
		view.pending = nil
		view.mu.Unlock()

		frame, _ := json.Marshal(likeFrame{Version: 1, Type: "LIKE_UPDATE", Data: update})
		n, last := manager.sendRoomFrame(view, clients, &outbound{payload: frame, total: update}, true)
		view.sendingSince.Store(nil)
		result := "complete"
		if n == 0 {
			result = "no_recipient"
		} else if n < len(clients) {
			result = "partial"
		}
		manager.metrics.fanoutResults.WithLabelValues(result).Inc()
		if n > 0 && since != nil {
			manager.metrics.fanout.Observe(last.Sub(*since).Seconds())
		}
	}
}

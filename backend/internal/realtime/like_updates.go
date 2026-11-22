package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
)

// 每个有观众的场次只保存一份待推送快照。所有成员共享 manager.mu。
type audience struct {
	roomID      uint64
	clients     map[*connection]struct{}
	revision    int64
	pending     *likeUpdate
	people      map[string]*personalUpdate
	dirtyPeople map[string]*personalUpdate
	board       publicBoard
	wantedBoard int64
	sentBoard   int64
	lastTop     []BoardRow
}

type likeUpdate struct {
	RoomID    string `json:"room_id"`
	SessionID string `json:"session_id"`
	Count     string `json:"like_count"`
	Revision  string `json:"likes_revision"`
	Epoch     string `json:"projection_epoch"`
}

// QueueLikeUpdate 在 Redis 原子提交以后调用；较早完成却较晚返回的分区结果不能覆盖新值。
func (manager *Manager) QueueLikeUpdate(event messaging.LikeEvent, projection messaging.ProjectedLike) {
	if projection.Duplicate {
		return
	}
	sessionID, err := strconv.ParseUint(event.SessionID, 10, 64)
	if err != nil {
		return
	}
	revision, err := strconv.ParseInt(projection.Revision, 10, 64)
	if err != nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	view := manager.audiences[sessionID]
	if view == nil || !manager.available.Load() {
		return
	}
	// 不同分区的返回可乱序。个人更新按照该用户自己的版本独立比较。
	if person := view.people[event.UserID]; person != nil && revision > person.revision {
		person.revision, person.count = revision, projection.Personal
		view.dirtyPeople[event.UserID] = person
	}
	if projection.BoardRevision == projection.Revision && revision > view.wantedBoard {
		view.wantedBoard = revision
	}
	if revision <= view.revision {
		return
	}
	view.revision = revision
	view.pending = &likeUpdate{RoomID: event.RoomID, SessionID: event.SessionID,
		Count: projection.Count, Revision: projection.Revision, Epoch: manager.epoch}
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
			manager.flushLikeUpdates()
		}
	}
}

func (manager *Manager) flushLikeUpdates() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, view := range manager.audiences {
		if view.pending == nil {
			continue
		}
		frame, _ := json.Marshal(map[string]any{"version": 1, "type": "LIKE_UPDATE", "data": view.pending})
		view.pending = nil
		for client := range view.clients {
			client.enqueue(frame)
		}
	}
}

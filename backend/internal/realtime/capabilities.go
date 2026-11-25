package realtime

import (
	"context"
	"strconv"
	"time"
)

// roomState 在无点赞依赖时仍能形成有效连接首屏；未知计数不用零代替。
func (manager *Manager) roomState(ctx context.Context, roomID, sessionID uint64, probe bool) []byte {
	current := manager.current.Load()
	var count, revision, epoch any
	ready := manager.available.Load() && current.projection != nil && current.initializer != nil
	if ready {
		check, cancel := context.WithTimeout(ctx, time.Second)
		err := current.initializer.EnsureSession(check, roomID, sessionID)
		if err == nil {
			snapshot, readErr := current.projection.ReadSession(check, strconv.FormatUint(roomID, 10), strconv.FormatUint(sessionID, 10))
			err = readErr
			if err == nil {
				count, revision, epoch = snapshot.Count, snapshot.Revision, current.epoch
			}
		}
		cancel()
		if err != nil {
			ready = false
			manager.SetAvailable(false)
		}
	}
	service := manager.danmaku.Load()
	enabled := service != nil
	canSend := enabled && service.Available()
	modes := map[string]any{"LIKE": nil, "DANMAKU": nil}
	if manager.likesEnabled.Load() {
		modes["LIKE"] = "kafka"
	}
	if enabled {
		modes["DANMAKU"] = "kafka"
	}
	raw, _ := encodeFrame("ROOM_STATE", map[string]any{
		"room_id": strconv.FormatUint(roomID, 10), "session_id": strconv.FormatUint(sessionID, 10), "status": "OPEN",
		"like_count": count, "likes_available": ready, "likes_state": manager.State(), "projection_epoch": epoch, "projection_generation": strconv.FormatUint(current.generation, 10), "likes_revision": revision,
		"danmaku_available": canSend, "history_available": enabled, "delivery_modes": modes, "delivery_probe": probe,
	})
	return raw
}

// RunCapabilities 统一且有界地刷新能力和恢复快照，不为状态变更创建无界协程。
func (manager *Manager) RunCapabilities(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	previous := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		service := manager.danmaku.Load()
		canSend := service != nil && service.Available()
		current := manager.current.Load()
		signature := manager.State() + ":" + current.epoch + ":" + strconv.FormatUint(current.generation, 10) + ":" + strconv.FormatBool(canSend)
		if signature == previous {
			continue
		}
		check, cancel := context.WithTimeout(ctx, 5*time.Second)
		for session, view := range manager.roomList() {
			if check.Err() != nil {
				break
			}
			raw := manager.roomState(check, view.roomID, session, false)
			view.mu.Lock()
			clients := copyClients(view.clients)
			closed := view.closed.Load()
			view.mu.Unlock()
			if closed {
				continue
			}
			for _, client := range clients {
				client.enqueue(raw)
			}
		}
		complete := check.Err() == nil
		cancel()
		if complete {
			previous = signature
		}
	}
}

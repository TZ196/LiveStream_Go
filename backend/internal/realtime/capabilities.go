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
	online := manager.online(roomID, sessionID)
	raw, _ := encodeFrame("ROOM_STATE", map[string]any{
		"room_id": strconv.FormatUint(roomID, 10), "session_id": strconv.FormatUint(sessionID, 10), "status": "OPEN",
		"online_count": online.Count, "server_instance_id": online.InstanceID, "presence_revision": online.Revision,
		"like_count": count, "likes_available": ready, "likes_state": manager.State(), "projection_epoch": epoch, "projection_generation": strconv.FormatUint(current.generation, 10), "likes_revision": revision,
		"danmaku_available": canSend, "history_available": enabled, "delivery_modes": modes, "delivery_probe": probe,
	})
	return raw
}

// RoomStats 读取本机在线人数和已有点赞快照，统计查询不初始化或修改投影。
func (manager *Manager) RoomStats(ctx context.Context, roomID, sessionID uint64) map[string]any {
	online := manager.online(roomID, sessionID)
	data := map[string]any{
		"room_id": online.RoomID, "session_id": online.SessionID, "online_count": online.Count,
		"server_instance_id": online.InstanceID, "presence_revision": online.Revision,
		"as_of":           time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"likes_available": false, "like_count": nil, "projection_epoch": nil, "likes_revision": nil,
	}
	current := manager.current.Load()
	if sessionID != 0 && manager.available.Load() && current.projection != nil {
		check, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		snapshot, err := current.projection.ReadSession(check, online.RoomID, strconv.FormatUint(sessionID, 10))
		if err == nil && manager.available.Load() && manager.current.Load() == current {
			data["likes_available"] = true
			data["like_count"], data["projection_epoch"], data["likes_revision"] = snapshot.Count, current.epoch, snapshot.Revision
		}
	}
	return data
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

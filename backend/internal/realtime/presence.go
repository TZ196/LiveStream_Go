package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type roomMember struct {
	owner     ownershipRecord
	sessionID uint64
	view      *audience
	client    *connection
}

// ConfigureOwnership 在 API 开始监听前设置归属仓库和实际连接配额。
func (m *Manager) ConfigureOwnership(client *redis.Client, connectionLimit int) {
	m.ownership = &ownershipStore{redis: client}
	m.connectionLimit = connectionLimit
}

// reserveConnection 和停机共用准入锁。名额从握手开始保留到读写协程退出。
func (m *Manager) reserveConnection(userID string) int {
	m.admission.RLock()
	defer m.admission.RUnlock()
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	if m.stopping || m.ownership == nil || m.heldConnections >= m.connectionLimit {
		return 503
	}
	if _, pending := m.handshakes[userID]; pending {
		return 429
	}
	m.handshakes[userID] = struct{}{}
	m.heldConnections++
	m.connectionWait.Add(1)
	return 0
}

func (m *Manager) endHandshake(userID string) {
	m.presenceMu.Lock()
	delete(m.handshakes, userID)
	m.presenceMu.Unlock()
}

func (m *Manager) freeConnection() {
	m.presenceMu.Lock()
	m.heldConnections--
	m.presenceMu.Unlock()
	m.connectionWait.Done()
}

// activate 串行修改账号索引和人数。先停用旧连接，后续 socket 写入不持房间锁。
func (m *Manager) activate(roomID, sessionID uint64, owner ownershipRecord, client *connection) *roomMember {
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	previous := m.members[owner.UserID]
	if previous != nil {
		previous.client.stop("replaced")
	}
	view := m.register(roomID, sessionID, owner.UserID, client)
	// 同场换连接时保留 audience，人数、队列和本场最新广播状态无需重建。
	if previous != nil {
		m.removeClient(previous.sessionID, previous.view, owner.UserID, previous.client)
		if previous.view != view {
			m.markOnlineChanged(previous.view)
		}
	}
	if previous == nil || previous.view != view {
		m.markOnlineChanged(view)
	}
	member := &roomMember{owner: owner, sessionID: sessionID, view: view, client: client}
	m.members[owner.UserID] = member
	return member
}

func (m *Manager) deactivate(member *roomMember, reason string) {
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	m.deactivateLocked(member, reason)
}

func (m *Manager) deactivateLocked(member *roomMember, reason string) {
	if m.members[member.owner.UserID] != member {
		return
	}
	member.client.stop(reason)
	delete(m.members, member.owner.UserID)
	m.removeClient(member.sessionID, member.view, member.owner.UserID, member.client)
	m.markOnlineChanged(member.view)
}

func (m *Manager) markOnlineChanged(view *audience) {
	m.onlineRevision++
	view.onlineRevision = m.onlineRevision
	view.onlineDirty = true
}

type onlineSnapshot struct {
	RoomID     string `json:"room_id"`
	SessionID  any    `json:"session_id"`
	Count      int    `json:"online_count"`
	InstanceID string `json:"server_instance_id"`
	Revision   string `json:"presence_revision"`
}

func (m *Manager) online(roomID, sessionID uint64) onlineSnapshot {
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	return m.onlineLocked(roomID, sessionID, m.lookup(sessionID))
}

func (m *Manager) onlineLocked(roomID, sessionID uint64, view *audience) onlineSnapshot {
	snapshot := onlineSnapshot{RoomID: strconv.FormatUint(roomID, 10), InstanceID: m.instanceID, Revision: strconv.FormatUint(m.onlineRevision, 10)}
	if sessionID != 0 {
		snapshot.SessionID = strconv.FormatUint(sessionID, 10)
	}
	if view != nil && !view.closed.Load() {
		view.mu.Lock()
		snapshot.Count = len(view.people)
		view.mu.Unlock()
		snapshot.Revision = strconv.FormatUint(view.onlineRevision, 10)
	}
	return snapshot
}

func (m *Manager) flushOnline() {
	type update struct {
		snapshot onlineSnapshot
		clients  []*connection
	}
	var updates []update
	m.presenceMu.Lock()
	// 慢连接可能还困在最后一次写入，已经停用的连接及时退出人数统计。
	for _, member := range m.members {
		if !member.client.accepting() {
			m.deactivateLocked(member, "peer_closed")
		}
	}
	for id, view := range m.roomList() {
		if !view.onlineDirty || view.closed.Load() {
			continue
		}
		view.onlineDirty = false
		snapshot := m.onlineLocked(view.roomID, id, view)
		view.mu.Lock()
		clients := copyClients(view.clients)
		view.mu.Unlock()
		updates = append(updates, update{snapshot, clients})
	}
	m.presenceMu.Unlock()
	for _, changed := range updates {
		raw, _ := encodeFrame("ROOM_ONLINE_UPDATE", changed.snapshot)
		message := &outbound{payload: raw}
		for _, client := range changed.clients {
			client.delivery.Lock()
			client.offer(message)
			client.delivery.Unlock()
		}
	}
}

// RunPresence 合并人数更新。续期和通知各有一个进程级循环，Redis 等待不占用人数推送。
func (m *Manager) RunPresence(ctx context.Context) {
	var background sync.WaitGroup
	background.Go(func() { m.listenReplacements(ctx) })
	background.Go(func() { m.runRenewal(ctx) })
	defer background.Wait()
	online := time.NewTicker(time.Second)
	defer online.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-online.C:
			m.flushOnline()
		}
	}
}

func (m *Manager) runRenewal(ctx context.Context) {
	ticker := time.NewTicker(ownerRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.renewConnections(ctx)
		}
	}
}

func (m *Manager) listenReplacements(ctx context.Context) {
	pubsub := m.ownership.redis.Subscribe(ctx, replaceChannel(m.instanceID))
	defer pubsub.Close()
	messages := pubsub.Channel(redis.WithChannelSize(64))
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-messages:
			if !ok {
				return
			}
			var previous ownershipRecord
			if json.Unmarshal([]byte(message.Payload), &previous) != nil {
				continue
			}
			m.presenceMu.Lock()
			member := m.members[previous.UserID]
			if member != nil && member.owner == previous {
				m.deactivateLocked(member, "replaced")
			}
			m.presenceMu.Unlock()
		}
	}
}

func (m *Manager) renewConnections(parent context.Context) {
	m.presenceMu.Lock()
	members := make([]*roomMember, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}
	m.presenceMu.Unlock()
	for offset := 0; offset < len(members) && parent.Err() == nil; offset += ownerRenewBatch {
		if !m.renewBatch(parent, members[offset:min(offset+ownerRenewBatch, len(members))]) {
			return
		}
	}
}

func (m *Manager) renewBatch(parent context.Context, members []*roomMember) bool {
	ctx, cancel := context.WithTimeout(parent, ownerRedisTimeout)
	defer cancel()
	started := time.Now()
	pipe := m.ownership.redis.Pipeline()
	commands := make([]*redis.Cmd, len(members))
	for i, member := range members {
		owner := member.owner
		if !member.client.accepting() {
			continue
		}
		// 使用 EVAL，Redis 刚重启时也不依赖其内存脚本缓存。
		commands[i] = renewOwner.Eval(ctx, pipe, []string{ownerKey(owner.UserID)}, owner.RunID, owner.InstanceID,
			owner.ConnectionID, owner.Generation, ownerLease.Milliseconds())
	}
	_, err := pipe.Exec(ctx)
	for i, command := range commands {
		if command == nil {
			continue
		}
		result, err := command.Int()
		if err != nil {
			continue
		} // 只在确认续期后延长本地期限。
		member := members[i]
		switch result {
		case 1:
			member.client.extendLease(started.Add(ownerLease))
		case 2:
			m.deactivate(member, "replaced")
		default:
			m.deactivate(member, "watch_expired")
		}
	}
	return err == nil
}

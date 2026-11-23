package realtime

import (
	"context"
	"maps"
)

// 容量为一的令牌用于互斥，消费路径可在业务期限到达时退出等待。
type roomMutex chan struct{}

func (mu roomMutex) Lock()   { mu <- struct{}{} }
func (mu roomMutex) Unlock() { <-mu }
func (mu roomMutex) TryLock() bool {
	select {
	case mu <- struct{}{}:
		return true
	default:
		return false
	}
}
func (mu roomMutex) LockContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case mu <- struct{}{}:
		// 令牌与取消同时就绪时仍遵守调用方的期限。
		if err := ctx.Err(); err != nil {
			mu.Unlock()
			return err
		}
		return nil
	}
}

func newAudience(roomID uint64) *audience {
	return &audience{
		mu:          make(roomMutex, 1),
		roomID:      roomID,
		clients:     make(map[*connection]struct{}),
		people:      make(map[string]*personalUpdate),
		dirtyPeople: make(map[string]*personalUpdate),
	}
}

func (manager *Manager) lookup(sessionID uint64) *audience {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.audiences[sessionID]
}

func (manager *Manager) roomList() map[uint64]*audience {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return maps.Clone(manager.audiences)
}

func copyClients(clients map[*connection]struct{}) []*connection {
	list := make([]*connection, 0, len(clients))
	for client := range clients {
		list = append(list, client)
	}
	return list
}

// 先封闭旧对象，再按指针移除目录项，延迟的清理不能删除同场次的新对象。
func (manager *Manager) remove(sessionID uint64, view *audience) {
	manager.mu.Lock()
	if manager.audiences[sessionID] == view {
		delete(manager.audiences, sessionID)
	}
	manager.mu.Unlock()
}

// 调用方持有 admission，关房、停机和恢复不会在登记途中变更准入。
// 最后一个观众仍可能同时退出，遇到旧对象则重新查找。
func (manager *Manager) register(roomID, sessionID uint64, userID string, client *connection) *audience {
	for {
		view := manager.lookup(sessionID)
		if view == nil || view.closed.Load() {
			manager.mu.Lock()
			view = manager.audiences[sessionID]
			if view == nil || view.closed.Load() {
				view = newAudience(roomID)
				manager.audiences[sessionID] = view
			}
			manager.mu.Unlock()
		}

		view.mu.Lock()
		if view.closed.Load() {
			view.mu.Unlock()
			continue
		}
		view.clients[client] = struct{}{}
		person := view.people[userID]
		if person == nil {
			person = &personalUpdate{clients: make(map[*connection]struct{})}
			view.people[userID] = person
		}
		person.clients[client] = struct{}{}
		view.mu.Unlock()
		return view
	}
}

func (manager *Manager) unregister(sessionID uint64, view *audience, userID string, client *connection) {
	view.mu.Lock()
	delete(view.clients, client)
	person := view.people[userID]
	if person != nil {
		delete(person.clients, client)
		if len(person.clients) == 0 {
			delete(view.people, userID)
			delete(view.dirtyPeople, userID)
		}
	}
	empty := len(view.clients) == 0
	if empty {
		view.closed.Store(true)
	}
	view.mu.Unlock()
	if empty {
		manager.remove(sessionID, view)
	}
	client.close("peer_closed")
}

// 摘除期间不嵌套目录锁与房间锁。物理断开由调用方在释放准入锁后执行。
func (manager *Manager) detachRooms(roomID uint64) []*connection {
	var clients []*connection
	for id, view := range manager.roomList() {
		if roomID != 0 && view.roomID != roomID {
			continue
		}
		view.mu.Lock()
		view.closed.Store(true)
		for client := range view.clients {
			clients = append(clients, client)
		}
		view.mu.Unlock()
		manager.remove(id, view)
	}
	return clients
}

func closeClients(clients []*connection, reason string) {
	for _, client := range clients {
		client.close(reason)
	}
}

package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/gorilla/websocket"
)

// Manager 管理当前进程的房间连接，点赞只通过 Kafka 发布。
type Manager struct {
	Tickets     *Tickets
	rooms       *room.Service
	producer    *messaging.Producer
	projection  *messaging.ProjectionRepository
	initializer *messaging.ProjectionInitializer
	users       *user.Repository
	epoch       string
	available   atomic.Bool
	admission   sync.Mutex
	mu          sync.Mutex
	audiences   map[uint64]*audience
	stopping    bool
}

type connection struct {
	socket *websocket.Conn
	send   chan []byte
	done   chan struct{}
	once   sync.Once
}

type frame struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

// NewManager 组装房间查询、一次性票据和 Kafka Producer。
func NewManager(rooms *room.Service, producer *messaging.Producer, projection *messaging.ProjectionRepository, initializer *messaging.ProjectionInitializer, users *user.Repository, epoch string) *Manager {
	return &Manager{Tickets: NewTickets(), rooms: rooms, producer: producer, projection: projection, initializer: initializer, users: users, epoch: epoch, audiences: make(map[uint64]*audience)}
}

// close 关闭连接，允许多个路径同时调用。
func (client *connection) close() {
	client.once.Do(func() { close(client.done); _ = client.socket.Close() })
}

// enqueue 将消息放入有限队列；慢连接会被断开，避免拖慢消费。
func (client *connection) enqueue(payload []byte) {
	select {
	case client.send <- payload:
	default:
		client.close()
	}
}

// ServeHTTP 消费一次性票据，完成 WebSocket 握手并读取用户消息。
func (manager *Manager) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	roomText := strings.TrimPrefix(request.URL.Path, "/ws/rooms/")
	roomID, err := strconv.ParseUint(roomText, 10, 64)
	if err != nil || roomID == 0 || strconv.FormatUint(roomID, 10) != roomText || len(request.URL.Query()) != 1 || len(request.URL.Query()["ticket"]) != 1 {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	origin := request.Header.Get("Origin")
	proof, err := manager.Tickets.consume(request.URL.Query().Get("ticket"), roomID, origin)
	if err != nil {
		http.Error(writer, "invalid ticket", http.StatusUnauthorized)
		return
	}
	session, err := manager.rooms.ActiveSession(request.Context(), roomID)
	if err != nil || session.ID != proof.sessionID {
		http.Error(writer, "room closed", http.StatusConflict)
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == proof.origin }}
	socket, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	client := &connection{socket: socket, send: make(chan []byte, 128), done: make(chan struct{})}
	manager.admission.Lock()
	active, checkErr := manager.rooms.ActiveSession(context.Background(), roomID)
	if checkErr != nil || active.ID != session.ID {
		manager.admission.Unlock()
		client.close()
		return
	}
	manager.mu.Lock()
	if manager.stopping {
		manager.mu.Unlock()
		manager.admission.Unlock()
		client.close()
		return
	}
	view := manager.audiences[session.ID]
	if view == nil {
		view = &audience{
			roomID:      roomID,
			clients:     make(map[*connection]struct{}),
			people:      make(map[string]*personalUpdate),
			dirtyPeople: make(map[string]*personalUpdate),
		}
		manager.audiences[session.ID] = view
	}
	view.clients[client] = struct{}{}
	userID := strconv.FormatUint(proof.identity.UserID, 10)
	person := view.people[userID]
	if person == nil {
		person = &personalUpdate{clients: make(map[*connection]struct{})}
		view.people[userID] = person
	}
	person.clients[client] = struct{}{}
	manager.mu.Unlock()
	manager.admission.Unlock()
	defer func() {
		manager.mu.Lock()
		delete(view.clients, client)
		delete(person.clients, client)
		if len(person.clients) == 0 {
			delete(view.people, userID)
			delete(view.dirtyPeople, userID)
		}
		if len(view.clients) == 0 && manager.audiences[session.ID] == view {
			delete(manager.audiences, session.ID)
		}
		manager.mu.Unlock()
		client.close()
	}()
	go client.writeLoop(proof.identity.ExpiresAt)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), time.Second)
	var snapshot messaging.ProjectedLike
	snapshotErr := manager.initializer.EnsureSession(snapshotCtx, roomID, session.ID)
	if snapshotErr == nil {
		snapshot, snapshotErr = manager.projection.ReadSession(snapshotCtx, roomText, strconv.FormatUint(session.ID, 10))
	}
	snapshotCancel()
	if !manager.available.Load() {
		snapshotErr = errors.New("实时消费者未就绪")
	}
	var count any
	var revision any
	if snapshotErr == nil {
		count = snapshot.Count
		revision = snapshot.Revision
	}
	initial, _ := json.Marshal(map[string]any{"version": 1, "type": "ROOM_STATE", "data": map[string]any{"room_id": roomText, "session_id": strconv.FormatUint(session.ID, 10), "like_count": count, "likes_available": snapshotErr == nil, "projection_epoch": manager.epoch, "likes_revision": revision}})
	client.enqueue(initial)
	socket.SetReadLimit(8 * 1024)
	deadline := func() time.Time {
		next := time.Now().Add(60 * time.Second)
		if proof.identity.ExpiresAt.Before(next) {
			return proof.identity.ExpiresAt
		}
		return next
	}
	_ = socket.SetReadDeadline(deadline())
	socket.SetPongHandler(func(string) error { return socket.SetReadDeadline(deadline()) })
	for {
		kind, payload, err := socket.ReadMessage()
		if err != nil {
			return
		}
		if kind != websocket.TextMessage {
			return
		}
		var input frame
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF || input.Version != 1 || input.Type != "LIKE" || input.SessionID != strconv.FormatUint(session.ID, 10) {
			return
		}
		count, err := readLikeCount(input.Data)
		if err != nil {
			return
		}
		manager.admission.Lock()
		active, err := manager.rooms.ActiveSession(context.Background(), roomID)
		manager.admission.Unlock()
		if err != nil || active.ID != session.ID {
			return
		}
		event, err := messaging.NewLikeBatch(strconv.FormatUint(proof.identity.UserID, 10), roomText, input.SessionID, input.RequestID, count, time.Now())
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = manager.producer.PublishLike(ctx, event)
		cancel()
		if err != nil {
			continue
		}
		ack, _ := json.Marshal(map[string]any{"version": 1, "type": "ACK", "request_id": event.RequestID, "event_id": event.EventID, "data": map[string]any{"status": "accepted", "accepted_via": "kafka", "accepted_at": nil}})
		client.enqueue(ack)
	}
}

// readLikeCount 兼容旧客户端的空对象；新请求只接收正的 int64 点击数。
func readLikeCount(data json.RawMessage) (int64, error) {
	var payload messaging.LikePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0, err
	}
	if payload.Count == 0 {
		return 1, nil
	}
	return payload.Count, nil
}

// writeLoop 独占连接写入，周期性 ping 并在身份到期时关闭。
func (client *connection) writeLoop(expires time.Time) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	defer client.close()
	timer := time.NewTimer(time.Until(expires))
	defer timer.Stop()
	for {
		select {
		case payload := <-client.send:
			_ = client.socket.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if client.socket.WriteMessage(websocket.TextMessage, payload) != nil {
				return
			}
		case <-ticker.C:
			_ = client.socket.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if client.socket.WriteMessage(websocket.PingMessage, nil) != nil {
				return
			}
		case <-timer.C:
			return
		case <-client.done:
			return
		}
	}
}

// CloseRoom 关闭已结束房间的连接，一并移除尚待广播的旧场快照。
func (manager *Manager) CloseRoom(roomID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for id, view := range manager.audiences {
		if view.roomID == roomID {
			for client := range view.clients {
				client.close()
			}
			delete(manager.audiences, id)
		}
	}
}

// Close 关闭当前进程的房间连接。
func (manager *Manager) Close() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.stopping = true
	manager.disconnectAllLocked()
}

// Resume 让现有页面重新握手，读回暂停期间可能尚未推送的权威快照。
func (manager *Manager) Resume() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !manager.stopping {
		manager.available.Store(true)
		manager.disconnectAllLocked()
	}
}

func (manager *Manager) disconnectAllLocked() {
	for id, view := range manager.audiences {
		for client := range view.clients {
			client.close()
		}
		delete(manager.audiences, id)
	}
}

// LockAdmission 在房间关闭或重开时阻止新的连接和点赞准入。
func (manager *Manager) LockAdmission() { manager.admission.Lock() }

// UnlockAdmission 在房间状态和连接处理完成后释放准入锁。
func (manager *Manager) UnlockAdmission() { manager.admission.Unlock() }

// BroadcastUnavailable 告知在线连接实时计数暂停，不伪造旧计数为最新值。
func (manager *Manager) BroadcastUnavailable() {
	payload, _ := json.Marshal(map[string]any{"version": 1, "type": "LIKES_UNAVAILABLE", "data": map[string]any{}})
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, view := range manager.audiences {
		view.pending = nil
		for client := range view.clients {
			client.enqueue(payload)
		}
	}
}

// SetAvailable 记录实时消费是否处于可运行状态。
func (manager *Manager) SetAvailable(available bool) { manager.available.Store(available) }

// Health 检查 Redis、Kafka 及实时消费者状态，不代表 Worker 已落库。
func (manager *Manager) Health(ctx context.Context) (bool, bool, bool) {
	redisReady := manager.projection.Ping(ctx) == nil
	kafkaReady := manager.producer.Ping(ctx) == nil
	return redisReady, kafkaReady, manager.available.Load()
}

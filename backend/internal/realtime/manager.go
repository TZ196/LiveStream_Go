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
	admission   sync.RWMutex
	mu          sync.RWMutex
	audiences   map[uint64]*audience
	stopping    bool // 受 admission 保护
}

type connection struct {
	socket   *websocket.Conn
	send     chan []byte
	done     chan struct{}
	once     sync.Once
	delivery sync.Mutex // 排序本连接的业务帧与暂停通知，只覆盖非阻塞入队
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
	client.once.Do(func() {
		client.delivery.Lock()
		close(client.done)
		client.delivery.Unlock()
		_ = client.socket.Close()
	})
}

// enqueue 将消息放入有限队列；慢连接会被断开，避免拖慢消费。
func (client *connection) enqueue(payload []byte) {
	client.delivery.Lock()
	ok := client.offer(payload)
	client.delivery.Unlock()
	if !ok {
		client.close()
	}
}

// offer 只尝试入队，满队列交由调用者在释放锁后关闭。
func (client *connection) offer(payload []byte) bool {
	select {
	case <-client.done:
		return true
	default:
	}
	select {
	case client.send <- payload:
		return true
	default:
		return false
	}
}

// 普通帧与暂停通知经过同一条连接的投递门。暂停通知之后不会排入旧数据，
// 恢复时关闭旧 audience 的所有连接，新连接从 Redis 获取首屏。
func (manager *Manager) deliver(view *audience, clients []*connection, frame []byte) {
	manager.sendRoomFrame(view, clients, frame, true)
}

func (manager *Manager) sendRoomFrame(view *audience, clients []*connection, frame []byte, available bool) {
	for _, client := range clients {
		client.delivery.Lock()
		full := false
		if !view.closed.Load() && manager.available.Load() == available {
			full = !client.offer(frame)
		}
		client.delivery.Unlock()
		if full {
			client.close()
		}
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
	manager.admission.RLock()
	active, checkErr := manager.rooms.ActiveSession(context.Background(), roomID)
	if checkErr != nil || active.ID != session.ID {
		manager.admission.RUnlock()
		client.close()
		return
	}
	if manager.stopping {
		manager.admission.RUnlock()
		client.close()
		return
	}
	userID := strconv.FormatUint(proof.identity.UserID, 10)
	view := manager.register(roomID, session.ID, userID, client)
	manager.admission.RUnlock()
	defer manager.unregister(session.ID, view, userID, client)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), time.Second)
	var snapshot messaging.ProjectedLike
	snapshotErr := manager.initializer.EnsureSession(snapshotCtx, roomID, session.ID)
	if snapshotErr == nil {
		snapshot, snapshotErr = manager.projection.ReadSession(snapshotCtx, roomText, strconv.FormatUint(session.ID, 10))
	}
	snapshotCancel()
	if snapshotErr != nil && manager.available.Load() {
		// 首屏读取失败时重新握手，即使没有后续点赞也会再次读取权威计数。
		return
	}
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
	// 注册到读取 Redis 之间的更新已进入有界队列，先写首屏，再由写协程按序排空。
	go client.writeLoop(proof.identity.ExpiresAt, initial)
	if snapshotErr != nil {
		// 暂停后才加入的连接可能错过了该场已发送的暂停通知。
		manager.sendRoomFrame(view, []*connection{client}, unavailableFrame, false)
	}
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
		manager.admission.RLock()
		active, err := manager.rooms.ActiveSession(context.Background(), roomID)
		manager.admission.RUnlock()
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
func (client *connection) writeLoop(expires time.Time, initial []byte) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	defer client.close()
	_ = client.socket.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := client.socket.WriteMessage(websocket.TextMessage, initial); err != nil {
		return
	}
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

// DetachRoom 在关房准入区内摘除旧场次，返回的清理函数可在释放准入锁后调用。
func (manager *Manager) DetachRoom(roomID uint64) func() {
	if roomID == 0 {
		return func() {}
	}
	clients := manager.detachRooms(roomID)
	return func() { closeClients(clients) }
}

// CloseRoom 供直接关闭连接的调用方使用，也可在已有准入保护下调用。
func (manager *Manager) CloseRoom(roomID uint64) {
	disconnect := manager.DetachRoom(roomID)
	disconnect()
}

func (manager *Manager) Close() {
	manager.admission.Lock()
	manager.stopping = true
	manager.available.Store(false)
	clients := manager.detachRooms(0)
	manager.admission.Unlock()
	closeClients(clients)
}

// Resume 先隔离旧代次，再允许新连接使用 Redis 权威首屏。
func (manager *Manager) Resume() {
	manager.admission.Lock()
	if manager.stopping {
		manager.admission.Unlock()
		return
	}
	clients := manager.detachRooms(0)
	manager.available.Store(true)
	manager.admission.Unlock()
	closeClients(clients)
}

// LockAdmission 在房间关闭或重开时阻止新的连接和点赞准入。
func (manager *Manager) LockAdmission() { manager.admission.Lock() }

// UnlockAdmission 在房间状态和连接处理完成后释放准入锁。
func (manager *Manager) UnlockAdmission() { manager.admission.Unlock() }

var unavailableFrame = []byte(`{"version":1,"type":"LIKES_UNAVAILABLE","data":{}}`)

// BroadcastUnavailable 告知在线连接实时计数暂停，不伪造旧计数为最新值。
func (manager *Manager) BroadcastUnavailable() {
	for _, view := range manager.roomList() {
		if !view.mu.TryLock() {
			continue
		}
		if view.closed.Load() || manager.available.Load() || view.pauseNotified {
			view.mu.Unlock()
			continue
		}
		view.pauseNotified = true
		view.pending = nil
		clients := copyClients(view.clients)
		view.mu.Unlock()
		manager.sendRoomFrame(view, clients, unavailableFrame, false)
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

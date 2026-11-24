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

	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/gorilla/websocket"
)

// Manager 管理当前进程的房间连接，点赞只通过 Kafka 发布。
type Manager struct {
	Tickets     *Tickets
	rooms       *room.Service
	producer    *likes.Producer
	projection  *likes.ProjectionRepository
	initializer *likes.ProjectionInitializer
	users       *user.Repository
	epoch       string
	available   atomic.Bool
	admission   sync.RWMutex
	mu          sync.RWMutex
	audiences   map[uint64]*audience
	metrics     *deliveryMetrics
	connections atomic.Uint64
	stopping    bool // 受 admission 保护
}

type frame struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

// NewManager 组装房间查询、一次性票据和 Kafka Producer。
func NewManager(rooms *room.Service, producer *likes.Producer, projection *likes.ProjectionRepository, initializer *likes.ProjectionInitializer, users *user.Repository, epoch string) *Manager {
	manager := &Manager{Tickets: NewTickets(), rooms: rooms, producer: producer, projection: projection, initializer: initializer, users: users, epoch: epoch, audiences: make(map[uint64]*audience)}
	manager.metrics = newDeliveryMetrics(manager)
	return manager
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
	client := manager.newConnection(socket)
	manager.admission.RLock()
	active, checkErr := manager.rooms.ActiveSession(context.Background(), roomID)
	if checkErr != nil || active.ID != session.ID {
		manager.admission.RUnlock()
		client.close("admission")
		return
	}
	if manager.stopping {
		manager.admission.RUnlock()
		client.close("admission")
		return
	}
	userID := strconv.FormatUint(proof.identity.UserID, 10)
	view := manager.register(roomID, session.ID, userID, client)
	manager.admission.RUnlock()
	defer manager.unregister(session.ID, view, userID, client)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), time.Second)
	var snapshot likes.ProjectedLike
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
	initial, _ := json.Marshal(map[string]any{"version": 1, "type": "ROOM_STATE", "data": map[string]any{"room_id": roomText, "session_id": strconv.FormatUint(session.ID, 10), "like_count": count, "likes_available": snapshotErr == nil, "projection_epoch": manager.epoch, "likes_revision": revision, "delivery_probe": client.probe != nil}})
	// 注册到读取 Redis 之间的更新已进入有界队列，先写首屏，再由写协程按序排空。
	go client.writeLoop(proof.identity.ExpiresAt, initial)
	if snapshotErr != nil {
		// 暂停后才加入的连接可能错过了该场已发送的暂停通知。
		manager.sendRoomFrame(view, []*connection{client}, &outbound{payload: unavailableFrame}, false)
	}
	manager.readMessages(client, proof)
}

// readMessages 在握手和首屏写协程建立后运行，负责单连接读帧、准入和接受确认。
func (manager *Manager) readMessages(client *connection, proof ticket) {
	socket := client.socket
	roomID := proof.roomID
	roomText := strconv.FormatUint(roomID, 10)
	socket.SetReadLimit(8 * 1024)
	deadline := func() time.Time {
		next := time.Now().Add(60 * time.Second)
		if proof.identity.ExpiresAt.Before(next) {
			return proof.identity.ExpiresAt
		}
		return next
	}
	_ = socket.SetReadDeadline(deadline())
	// Gorilla 在读循环收到控制 Pong 时刷新读期限，期限同时受登录到期约束。
	socket.SetPongHandler(func(string) error { return socket.SetReadDeadline(deadline()) })
	sessionText := strconv.FormatUint(proof.sessionID, 10)
	var lastPing time.Time
	for {
		kind, payload, err := socket.ReadMessage()
		if err != nil {
			return
		}
		// 写协程可能还在完成最后一次 socket 写。逻辑关闭后不再准入新收到的业务帧。
		select {
		case <-client.done:
			return
		default:
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
		if decoder.Decode(&extra) != io.EOF || input.Version != 1 || input.SessionID != sessionText {
			return
		}
		switch input.Type {
		case "PING", "DELIVERY_RECEIPT":
			client.handleDeliveryControl(input, &lastPing)
			continue
		case "LIKE":
		default:
			return
		}
		count, err := readLikeCount(input.Data)
		if err != nil {
			return
		}
		manager.admission.RLock()
		active, err := manager.rooms.ActiveSession(context.Background(), roomID)
		manager.admission.RUnlock()
		if err != nil || active.ID != proof.sessionID {
			return
		}
		event, err := likes.NewLikeBatch(strconv.FormatUint(proof.identity.UserID, 10), roomText, input.SessionID, input.RequestID, count, time.Now())
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
	var payload likes.LikePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0, err
	}
	if payload.Count == 0 {
		return 1, nil
	}
	return payload.Count, nil
}

// DetachRoom 在关房准入区内摘除旧场次，返回的清理函数可在释放准入锁后调用。
func (manager *Manager) DetachRoom(roomID uint64) func() {
	if roomID == 0 {
		return func() {}
	}
	clients := manager.detachRooms(roomID)
	return func() { closeClients(clients, "room_closed") }
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
	closeClients(clients, "shutdown")
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
	closeClients(clients, "recovery")
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
		view.pendingSince.Store(nil)
		clients := copyClients(view.clients)
		view.mu.Unlock()
		manager.sendRoomFrame(view, clients, &outbound{payload: unavailableFrame}, false)
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

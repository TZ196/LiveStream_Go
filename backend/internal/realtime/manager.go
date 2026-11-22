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
	"github.com/gorilla/websocket"
)

// Manager 管理当前进程的房间连接，点赞只通过 Kafka 发布。
type Manager struct {
	Tickets    *Tickets
	rooms      *room.Service
	producer   *messaging.Producer
	projection *messaging.ProjectionRepository
	epoch      string
	available  atomic.Bool
	admission  sync.Mutex
	mu         sync.Mutex
	clients    map[*connection]struct{}
}

type connection struct {
	socket    *websocket.Conn
	roomID    uint64
	sessionID uint64
	send      chan []byte
	done      chan struct{}
	once      sync.Once
}

type frame struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

// NewManager 组装房间查询、一次性票据和 Kafka Producer。
func NewManager(rooms *room.Service, producer *messaging.Producer, projection *messaging.ProjectionRepository, epoch string) *Manager {
	return &Manager{Tickets: NewTickets(), rooms: rooms, producer: producer, projection: projection, epoch: epoch, clients: make(map[*connection]struct{})}
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
	client := &connection{socket: socket, roomID: roomID, sessionID: session.ID, send: make(chan []byte, 128), done: make(chan struct{})}
	manager.admission.Lock()
	active, checkErr := manager.rooms.ActiveSession(context.Background(), roomID)
	if checkErr != nil || active.ID != session.ID {
		manager.admission.Unlock()
		client.close()
		return
	}
	manager.mu.Lock()
	manager.clients[client] = struct{}{}
	manager.mu.Unlock()
	manager.admission.Unlock()
	defer func() { manager.mu.Lock(); delete(manager.clients, client); manager.mu.Unlock(); client.close() }()
	go client.writeLoop(proof.identity.ExpiresAt)
	snapshotCtx, snapshotCancel := context.WithTimeout(context.Background(), time.Second)
	snapshot, snapshotErr := manager.projection.ReadSession(snapshotCtx, roomText, strconv.FormatUint(session.ID, 10))
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
		if decoder.Decode(&extra) != io.EOF || input.Version != 1 || input.Type != "LIKE" || input.SessionID != strconv.FormatUint(session.ID, 10) || string(input.Data) != "{}" {
			return
		}
		manager.admission.Lock()
		active, err := manager.rooms.ActiveSession(context.Background(), roomID)
		manager.admission.Unlock()
		if err != nil || active.ID != session.ID {
			return
		}
		event, err := messaging.NewLikeEvent(strconv.FormatUint(proof.identity.UserID, 10), roomText, input.SessionID, input.RequestID, time.Now())
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

// BroadcastLike 仅向同一场次的在线连接广播实时消费者计算出的投影。
func (manager *Manager) BroadcastLike(event messaging.LikeEvent, projection messaging.ProjectedLike, epoch string) {
	if projection.Duplicate {
		return
	}
	payload, err := json.Marshal(map[string]any{"version": 1, "type": "LIKE_UPDATE", "event_id": event.EventID, "request_id": event.RequestID, "data": map[string]any{"room_id": event.RoomID, "session_id": event.SessionID, "like_count": projection.Count, "likes_revision": projection.Revision, "projection_epoch": epoch}})
	if err != nil {
		return
	}
	roomID, err := strconv.ParseUint(event.RoomID, 10, 64)
	if err != nil {
		return
	}
	sessionID, err := strconv.ParseUint(event.SessionID, 10, 64)
	if err != nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for client := range manager.clients {
		if client.roomID == roomID && client.sessionID == sessionID {
			client.enqueue(payload)
		}
	}
}

// CloseRoom 断开已经结束直播的房间连接，避免旧场次混入新场次。
func (manager *Manager) CloseRoom(roomID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for client := range manager.clients {
		if client.roomID == roomID {
			client.close()
		}
	}
}

// Close 关闭当前进程的所有 WebSocket 连接。
func (manager *Manager) Close() {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for client := range manager.clients {
		client.close()
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
	for client := range manager.clients {
		client.enqueue(payload)
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

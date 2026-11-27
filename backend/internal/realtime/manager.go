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
	"unicode/utf8"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Manager 管理共享房间连接、场次分发和独立业务能力；生命周期归 API 所有。
type Manager struct {
	Tickets         *Tickets
	rooms           *room.Service
	producer        *likes.Producer
	users           *user.Repository
	current         atomic.Pointer[projectionState]
	state           atomic.Value // CHECKING、READY、RECOVERING、BLOCKED；只存 string。
	available       atomic.Bool
	admission       sync.RWMutex
	likeAdmission   sync.RWMutex
	likesEnabled    atomic.Bool
	danmaku         atomic.Pointer[danmaku.Service]
	dispatchCtx     context.Context
	dispatchCancel  context.CancelFunc
	dispatchWait    sync.WaitGroup
	connectionWait  sync.WaitGroup
	mu              sync.RWMutex
	audiences       map[uint64]*audience
	metrics         *deliveryMetrics
	connections     atomic.Uint64
	stopping        bool // 受 admission 保护
	instanceID      string
	ownership       *ownershipStore // 启动前设置
	connectionLimit int
	presenceMu      sync.Mutex
	members         map[string]*roomMember // 下列字段在 presenceMu 下修改。
	handshakes      map[string]struct{}
	heldConnections int
	onlineRevision  uint64
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
	manager := &Manager{Tickets: NewTickets(), rooms: rooms, producer: producer, users: users, audiences: make(map[uint64]*audience)}
	manager.instanceID = uuid.NewString()
	manager.members = make(map[string]*roomMember)
	manager.handshakes = make(map[string]struct{})
	manager.current.Store(&projectionState{projection: projection, initializer: initializer, epoch: epoch, generation: 1})
	manager.state.Store("BLOCKED")
	if producer != nil || projection != nil {
		manager.likesEnabled.Store(true)
		manager.state.Store("CHECKING")
	}
	manager.dispatchCtx, manager.dispatchCancel = context.WithCancel(context.Background())
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
	userID := strconv.FormatUint(proof.identity.UserID, 10)
	if status := manager.reserveConnection(userID); status != 0 {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, "connection capacity exceeded", status)
		return
	}
	defer manager.freeConnection()
	reserved := true
	defer func() {
		if reserved {
			manager.endHandshake(userID)
		}
	}()

	check, cancel := context.WithTimeout(request.Context(), time.Second)
	session, err := manager.rooms.ActiveSession(check, roomID)
	cancel()
	if errors.Is(err, room.ErrUnavailable) {
		http.Error(writer, "room check unavailable", http.StatusServiceUnavailable)
		return
	}
	if err != nil || session.ID != proof.sessionID {
		http.Error(writer, "room closed", http.StatusConflict)
		return
	}
	connectionID, err := uuid.NewRandom()
	if err != nil {
		http.Error(writer, "connection unavailable", http.StatusServiceUnavailable)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: 3 * time.Second, CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == proof.origin }}
	socket, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	client := manager.newConnection(socket)
	manager.admission.RLock()
	check, cancel = context.WithTimeout(request.Context(), time.Second)
	active, err := manager.rooms.ActiveSession(check, roomID)
	cancel()
	if manager.stopping || err != nil || active.ID != session.ID {
		manager.admission.RUnlock()
		reason := "room_closed"
		if errors.Is(err, room.ErrUnavailable) {
			reason = "ownership_unavailable"
		}
		endSocket(socket, reason)
		return
	}
	owner := ownershipRecord{UserID: userID, InstanceID: manager.instanceID, ConnectionID: connectionID.String(),
		RoomID: roomText, SessionID: strconv.FormatUint(session.ID, 10), WatchID: proof.watch.watchID, RunID: proof.watch.runID, Active: true}
	confirmed, previous, deadline, err := manager.ownership.claim(request.Context(), owner, proof)
	if err != nil || !deadline.After(time.Now()) {
		manager.admission.RUnlock()
		if confirmed.Active {
			manager.ownership.release(confirmed)
		}
		reason := "ownership_unavailable"
		switch {
		case errors.Is(err, ErrWatchReplaced):
			reason = "replaced"
		case errors.Is(err, ErrWatchExpired):
			reason = "watch_expired"
		case !proof.identity.ExpiresAt.After(time.Now()):
			reason = "auth_expired"
		}
		endSocket(socket, reason)
		return
	}
	client.leaseUntil = deadline
	client.authUntil = proof.identity.ExpiresAt
	member := manager.activate(roomID, session.ID, confirmed, client)
	manager.admission.RUnlock()
	manager.endHandshake(userID)
	reserved = false
	defer manager.ownership.release(confirmed)
	if previous.InstanceID != manager.instanceID {
		manager.ownership.notifyReplacement(manager.dispatchCtx, previous)
	}
	initial := manager.roomState(request.Context(), roomID, session.ID, client.probe != nil)
	// 注册之后的增量已在有界连接队列中，首屏仍由唯一写协程最先发送。
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		client.writeLoop(initial)
		manager.deactivate(member, "peer_closed")
		// 跳过业务队列发送关闭原因，队列满时同样可以尝试通知。
		endSocket(socket, client.endReason)
		if client.probe != nil {
			client.expireProbe(time.Now(), true)
		}
	}()
	manager.readMessages(client, proof)
	manager.deactivate(member, "peer_closed")
	<-writerDone
}

// readMessages 在握手和首屏写协程建立后运行，负责单连接读帧、准入和接受确认。
func (manager *Manager) readMessages(client *connection, proof ticket) {
	socket := client.socket
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
		if kind != websocket.TextMessage || !utf8.Valid(payload) {
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
		if !client.accepting() {
			return
		}
		switch input.Type {
		case "PING", "DELIVERY_RECEIPT":
			client.handleDeliveryControl(input, &lastPing)
			continue
		case "DANMAKU":
			manager.receiveDanmaku(client, proof, input)
			continue
		case "LIKE":
			if !manager.receiveLike(client, proof, input) {
				return
			}
		default:
			return
		}
	}
}

// DetachRoom 在关房准入区内摘除旧场次，写循环收到退出信号后关闭 Socket。
func (manager *Manager) DetachRoom(roomID uint64) {
	if roomID == 0 {
		return
	}
	manager.detachRooms(roomID, "room_closed")
}

func (manager *Manager) Close() {
	manager.admission.Lock()
	manager.stopping = true
	manager.available.Store(false)
	manager.state.Store("RECOVERING")
	manager.detachRooms(0, "shutdown")
	manager.admission.Unlock()
	manager.dispatchCancel()
	manager.dispatchWait.Wait()
	manager.connectionWait.Wait()
}

// UnlockAdmission 在房间状态和连接处理完成后释放准入锁。
func (manager *Manager) UnlockAdmission() { manager.admission.Unlock() }

// BeginRoomChange 与恢复暂停互斥，避免构建快照时改变场次集合。
func (manager *Manager) BeginRoomChange() bool {
	manager.admission.Lock()
	if (manager.likesEnabled.Load() && !manager.available.Load()) || manager.stopping {
		manager.admission.Unlock()
		return false
	}
	return true
}

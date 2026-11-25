package realtime

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/gorilla/websocket"
)

// Manager 管理共享房间连接、场次分发和独立业务能力；生命周期归 API 所有。
type Manager struct {
	Tickets        *Tickets
	rooms          *room.Service
	producer       *likes.Producer
	users          *user.Repository
	current        atomic.Pointer[projectionState]
	state          atomic.Value // CHECKING、READY、RECOVERING、BLOCKED；只存 string。
	available      atomic.Bool
	admission      sync.RWMutex
	likeAdmission  sync.RWMutex
	likesEnabled   atomic.Bool
	danmaku        atomic.Pointer[danmaku.Service]
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	dispatchWait   sync.WaitGroup
	connectionWait sync.WaitGroup
	mu             sync.RWMutex
	audiences      map[uint64]*audience
	metrics        *deliveryMetrics
	connections    atomic.Uint64
	stopping       bool // 受 admission 保护
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
	check, cancel := context.WithTimeout(request.Context(), time.Second)
	active, err := manager.rooms.ActiveSession(check, roomID)
	cancel()
	if manager.stopping || err != nil || active.ID != session.ID {
		manager.admission.RUnlock()
		client.close("admission")
		return
	}
	userID := strconv.FormatUint(proof.identity.UserID, 10)
	view := manager.register(roomID, session.ID, userID, client)
	manager.connectionWait.Add(2)
	defer manager.connectionWait.Done()
	manager.admission.RUnlock()
	defer manager.unregister(session.ID, view, userID, client)
	initial := manager.roomState(request.Context(), roomID, session.ID, client.probe != nil)
	// 注册之后的增量已在有界连接队列中，首屏仍由唯一写协程最先发送。
	go func() { defer manager.connectionWait.Done(); client.writeLoop(proof.identity.ExpiresAt, initial) }()
	manager.readMessages(client, proof)
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
		// 写协程可能还在完成最后一次 socket 写。逻辑关闭后不再准入新收到的业务帧。
		select {
		case <-client.done:
			return
		default:
		}
		if kind != websocket.TextMessage || !utf8.Valid(payload) || !proof.identity.ExpiresAt.After(time.Now()) {
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
	manager.state.Store("RECOVERING")
	clients := manager.detachRooms(0)
	manager.admission.Unlock()
	closeClients(clients, "shutdown")
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

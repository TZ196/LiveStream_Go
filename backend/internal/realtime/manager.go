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

// 一次替换整个依赖快照，使 HTTP、广播和消费者读取到同一个投影代次。
type projectionState struct {
	projection  *likes.ProjectionRepository
	initializer *likes.ProjectionInitializer
	epoch       string
	generation  uint64
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
		default:
			return
		}
		count, err := readLikeCount(input.Data)
		if err != nil {
			return
		}
		event, err := likes.NewLikeBatch(strconv.FormatUint(proof.identity.UserID, 10), strconv.FormatUint(proof.roomID, 10), input.SessionID, input.RequestID, count, time.Now())
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = manager.publishLike(ctx, proof, event)
		cancel()
		if err != nil {
			continue
		}
		ack, _ := json.Marshal(map[string]any{"version": 1, "type": "ACK", "request_id": event.RequestID, "event_id": event.EventID, "data": map[string]any{"status": "accepted", "accepted_via": "kafka", "accepted_at": nil}})
		client.enqueue(ack)
	}
}

// publishLike 持有准入读锁直到发送结束；暂停取得写锁后，所有旧发送都已返回。
func (manager *Manager) publishLike(ctx context.Context, proof ticket, event likes.LikeEvent) error {
	manager.likeAdmission.RLock()
	defer manager.likeAdmission.RUnlock()
	if !manager.available.Load() || manager.producer == nil {
		return errors.New("点赞接收已暂停")
	}
	active, err := manager.rooms.ActiveSession(ctx, proof.roomID)
	if err != nil || active.ID != proof.sessionID {
		return errors.New("直播场次已改变")
	}
	return manager.producer.PublishLike(ctx, event)
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
	manager.state.Store("RECOVERING")
	clients := manager.detachRooms(0)
	manager.admission.Unlock()
	closeClients(clients, "shutdown")
	manager.dispatchCancel()
	manager.dispatchWait.Wait()
	manager.connectionWait.Wait()
}

// Resume 允许点赞准入；共享连接通过新首屏识别恢复后的投影版本。
func (manager *Manager) Resume() {
	manager.admission.RLock()
	stopped := manager.stopping
	manager.admission.RUnlock()
	if stopped {
		return
	}
	for _, view := range manager.roomList() {
		view.mu.Lock()
		view.pauseNotified = false
		view.mu.Unlock()
	}
	manager.available.Store(true)
	manager.state.Store("READY")
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
func (manager *Manager) SetAvailable(available bool) {
	manager.available.Store(available)
	if available {
		manager.state.Store("READY")
	} else {
		manager.state.Store("RECOVERING")
	}
}

func (manager *Manager) State() string { return manager.state.Load().(string) }

func (manager *Manager) Available() bool { return manager.available.Load() }

// Pause 只封闭点赞准入并等待本业务在途发布，保留共享 WebSocket 和弹幕。
func (manager *Manager) Pause(state string) {
	manager.available.Store(false)
	manager.state.Store(state)
	manager.admission.Lock()
	manager.admission.Unlock()
	manager.likeAdmission.Lock()
	manager.likeAdmission.Unlock()
}

func (manager *Manager) UseProjection(projection *likes.ProjectionRepository, initializer *likes.ProjectionInitializer) {
	old := manager.current.Load()
	if old.epoch != projection.Epoch() {
		for _, view := range manager.roomList() {
			view.board.mu.Lock()
			view.board.revision = ""
			view.board.rows = nil
			view.board.mu.Unlock()
			view.mu.Lock()
			view.revision = 0
			view.pending = nil
			view.pendingSince.Store(nil)
			view.sendingSince.Store(nil)
			view.boardSince.Store(nil)
			view.wantedBoard = 0
			view.sentBoard = 0
			view.lastTop = nil
			view.dirtyPeople = make(map[string]*personalUpdate)
			for _, person := range view.people {
				person.revision = 0
				person.count = ""
			}
			view.pauseNotified = false
			view.mu.Unlock()
		}
	}
	manager.current.Store(&projectionState{projection: projection, initializer: initializer, epoch: projection.Epoch(), generation: old.generation + 1})
	manager.Resume()
}

// ConfigureLikes 只在监听前组装；业务恢复随后原子替换投影快照。
func (manager *Manager) ConfigureLikes(producer *likes.Producer, projection *likes.ProjectionRepository, initializer *likes.ProjectionInitializer) {
	manager.producer = producer
	manager.likesEnabled.Store(true)
	manager.state.Store("CHECKING")
	manager.current.Store(&projectionState{projection: projection, initializer: initializer, epoch: projection.Epoch(), generation: 1})
}

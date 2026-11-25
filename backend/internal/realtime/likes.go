package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/likes"
)

// 一次替换整个依赖快照，使 HTTP、广播和消费者读取到同一个投影代次。
type projectionState struct {
	projection  *likes.ProjectionRepository
	initializer *likes.ProjectionInitializer
	epoch       string
	generation  uint64
}

// receiveLike 只做点赞帧校验和响应映射；返回 false 时结束不合法的连接读循环。
func (manager *Manager) receiveLike(client *connection, proof ticket, input frame) bool {
	count, err := readLikeCount(input.Data)
	if err != nil {
		return false
	}
	event, err := likes.NewLikeBatch(strconv.FormatUint(proof.identity.UserID, 10), strconv.FormatUint(proof.roomID, 10), input.SessionID, input.RequestID, count, time.Now())
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = manager.publishLike(ctx, proof, event)
	cancel()
	if err != nil {
		return true
	}
	ack, _ := json.Marshal(map[string]any{"version": 1, "type": "ACK", "request_id": event.RequestID, "event_id": event.EventID, "data": map[string]any{"status": "accepted", "accepted_via": "kafka", "accepted_at": nil}})
	client.enqueue(ack)
	return true
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
	manager.waitForRoomChanges()
	manager.waitForLikePublishes()
}

// waitForRoomChanges 等待已准入的房间操作结束，函数返回时立即释放共享锁。
func (manager *Manager) waitForRoomChanges() {
	manager.admission.Lock()
	defer manager.admission.Unlock()
}

// waitForLikePublishes 等待旧点赞发布收尾，此时不再占用共享房间锁。
func (manager *Manager) waitForLikePublishes() {
	manager.likeAdmission.Lock()
	defer manager.likeAdmission.Unlock()
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

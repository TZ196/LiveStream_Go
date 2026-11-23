package realtime

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
)

type BoardRow struct {
	Rank     int    `json:"rank"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Count    string `json:"count"`
}

type BoardMe struct {
	Count string `json:"count"`
	Rank  int    `json:"rank,omitempty"`
}

type BoardResponse struct {
	RoomID        string     `json:"room_id"`
	SessionID     string     `json:"session_id"`
	Epoch         string     `json:"projection_epoch"`
	Revision      string     `json:"revision"`
	LikesRevision string     `json:"likes_revision"`
	Top           []BoardRow `json:"top"`
	Me            BoardMe    `json:"me"`
}

// 单独串行化同场榜单的 Redis 读取与昵称补全。持此锁期间不再获取目录锁或房间锁。
// 每次替换整个前 100 名切片，调用方拿到的快照可在锁外读取。
type publicBoard struct {
	mu       sync.Mutex
	revision string
	rows     []BoardRow
}

var errBoardBusy = errors.New("本场榜单缓存正在读取")

type personalUpdate struct {
	revision int64
	count    string
	clients  map[*connection]struct{}
}

func (manager *Manager) readBoard(ctx context.Context, view *audience, roomID, sessionID, userID string) (BoardResponse, error) {
	cache := &publicBoard{}
	if view != nil {
		cache = &view.board
	}
	if userID == "" {
		// 广播读取可以留到下个周期，不在其他请求持有的缓存锁后排队。
		if !cache.mu.TryLock() {
			return BoardResponse{}, errBoardBusy
		}
	} else {
		cache.mu.Lock()
	}
	defer cache.mu.Unlock()
	snap, err := manager.projection.ReadBoard(ctx, roomID, sessionID, userID, cache.revision)
	if err != nil {
		return BoardResponse{}, err
	}
	if snap.Revision != cache.revision {
		rows, err := manager.resolveRows(ctx, snap.Top, cache.rows)
		if err != nil {
			return BoardResponse{}, err
		}
		cache.revision, cache.rows = snap.Revision, rows
	}
	answer := BoardResponse{
		RoomID: roomID, SessionID: sessionID, Epoch: manager.epoch,
		Revision: snap.Revision, LikesRevision: snap.LikesRevision,
		Top: cache.rows, Me: BoardMe{Count: snap.Personal},
	}
	for _, row := range cache.rows {
		if row.UserID == userID {
			answer.Me.Rank = row.Rank
			break
		}
	}
	return answer, nil
}

func (manager *Manager) resolveRows(ctx context.Context, ranked []messaging.RankedUser, previous []BoardRow) ([]BoardRow, error) {
	names := make(map[string]string, len(previous))
	for _, row := range previous {
		names[row.UserID] = row.Username
	}
	var missing []uint64
	for _, item := range ranked {
		if _, ok := names[item.UserID]; ok {
			continue
		}
		id, err := strconv.ParseUint(item.UserID, 10, 64)
		if err != nil {
			return nil, err
		}
		missing = append(missing, id)
	}
	if len(missing) > 0 {
		found, err := manager.users.Names(ctx, missing)
		if err != nil {
			return nil, err
		}
		for id, name := range found {
			names[id] = name
		}
	}
	rows := make([]BoardRow, 0, len(ranked))
	for i, item := range ranked {
		rows = append(rows, BoardRow{Rank: i + 1, UserID: item.UserID, Username: names[item.UserID], Count: item.Count})
	}
	return rows, nil
}

// Leaderboard 供通过登录和场次校验的 HTTP 请求读取，个人数据只属于当前请求。
func (manager *Manager) Leaderboard(ctx context.Context, roomID, sessionID, userID uint64) (BoardResponse, error) {
	if !manager.available.Load() {
		return BoardResponse{}, errors.New("实时点赞暂不可用")
	}
	if err := manager.initializer.EnsureSession(ctx, roomID, sessionID); err != nil {
		return BoardResponse{}, err
	}
	view := manager.lookup(sessionID)
	answer, err := manager.readBoard(ctx, view, strconv.FormatUint(roomID, 10), strconv.FormatUint(sessionID, 10), strconv.FormatUint(userID, 10))
	if err != nil || view == nil {
		return answer, err
	}
	version, err := strconv.ParseInt(answer.Revision, 10, 64)
	if err != nil {
		return BoardResponse{}, err
	}
	// 首个 HTTP 快照可作为差量起点。readBoard 已释放缓存锁，房间锁只保护分发进度。
	view.mu.Lock()
	if !view.closed.Load() && view.lastTop == nil && view.wantedBoard == 0 {
		view.sentBoard, view.lastTop = version, answer.Top
	}
	view.mu.Unlock()
	return answer, nil
}

// delta 仅发变化的名次；超过单帧预算时客户端按版本获取一次 HTTP 快照。
func boardDelta(old []BoardRow, base string, board BoardResponse) ([]byte, error) {
	changes := make([]BoardRow, 0)
	for i, row := range board.Top {
		if i >= len(old) || row != old[i] {
			changes = append(changes, row)
		}
	}
	data := map[string]any{
		"room_id": board.RoomID, "session_id": board.SessionID, "projection_epoch": board.Epoch,
		"revision": board.Revision, "base_revision": base, "size": len(board.Top), "changes": changes,
	}
	frame, err := encodeFrame("LIKE_BOARD_DELTA", data)
	if err != nil || len(frame) <= 8*1024 {
		return frame, err
	}
	return encodeFrame("LIKE_BOARD_REFRESH", map[string]string{"room_id": board.RoomID, "session_id": board.SessionID, "projection_epoch": board.Epoch, "revision": board.Revision})
}

func (manager *Manager) RunLeaderboard(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			manager.flushLeaderboard(ctx)
		}
	}
}

type personalSnapshot struct {
	revision int64
	count    string
	clients  []*connection
}

func (manager *Manager) flushLeaderboard(ctx context.Context) {
	for sessionID, view := range manager.roomList() {
		if ctx.Err() != nil || !manager.available.Load() {
			return
		}
		manager.flushRoomLeaderboard(ctx, sessionID, view)
	}
}

func (manager *Manager) flushRoomLeaderboard(ctx context.Context, sessionID uint64, view *audience) {
	// 忙场次的待发标记仍留在房间，下一轮再取，定时器可继续处理其他场次。
	if !view.mu.TryLock() {
		manager.metrics.skips.WithLabelValues("board_room_busy").Inc()
		return
	}
	if view.closed.Load() || !manager.available.Load() {
		view.mu.Unlock()
		return
	}
	// 个人数、版本和收件人必须属于同一时刻。取走后，新点赞会重新标记该用户。
	personal := make([]personalSnapshot, 0, len(view.dirtyPeople))
	for id, update := range view.dirtyPeople {
		personal = append(personal, personalSnapshot{update.revision, update.count, copyClients(update.clients)})
		delete(view.dirtyPeople, id)
	}
	base, previous := view.sentBoard, view.lastTop
	needsBoard := view.wantedBoard > base
	view.mu.Unlock()

	sessionText := strconv.FormatUint(sessionID, 10)
	for _, update := range personal {
		frame, err := encodeFrame("MY_LIKES_UPDATE", map[string]string{
			"session_id": sessionText, "projection_epoch": manager.epoch,
			"likes_revision": strconv.FormatInt(update.revision, 10), "count": update.count,
		})
		if err != nil {
			slog.Error("encode_personal_likes", "error", err)
			continue
		}
		manager.sendRoomFrame(view, update.clients, &outbound{payload: frame}, true)
	}
	if !needsBoard {
		return
	}
	read, cancel := context.WithTimeout(ctx, time.Second)
	board, err := manager.readBoard(read, view, strconv.FormatUint(view.roomID, 10), sessionText, "")
	cancel()
	if err != nil {
		// 只在分发成功生成差量后推进版本，读失败可在下一轮重试。
		reason := "board_read_error"
		if errors.Is(err, errBoardBusy) {
			reason = "board_cache_busy"
		}
		manager.metrics.skips.WithLabelValues(reason).Inc()
		return
	}
	version, err := strconv.ParseInt(board.Revision, 10, 64)
	if err != nil || version <= base {
		return
	}
	frame, err := boardDelta(previous, strconv.FormatInt(base, 10), board)
	if err != nil {
		slog.Error("encode_board_delta", "error", err)
		return
	}
	if !view.mu.TryLock() {
		manager.metrics.skips.WithLabelValues("board_room_busy").Inc()
		return
	}
	if view.closed.Load() || !manager.available.Load() || view.sentBoard != base {
		view.mu.Unlock()
		return
	}
	clients := copyClients(view.clients)
	view.lastTop, view.sentBoard = board.Top, version
	if version >= view.wantedBoard {
		view.boardSince.Store(nil)
	}
	view.mu.Unlock()
	manager.sendRoomFrame(view, clients, &outbound{payload: frame}, true)
}

package realtime

import (
	"context"
	"encoding/json"
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

// 只随在线场次缓存最多 100 个公开条目。每次替换整个切片，发出的快照不可变。
type publicBoard struct {
	mu       sync.Mutex
	revision string
	rows     []BoardRow
}

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
	cache.mu.Lock()
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
	// 第一个首屏快照同时成为本场分发差量的起点。持有场次缓存锁保证首屏之间的顺序。
	if view != nil && userID != "" {
		version, err := strconv.ParseInt(snap.Revision, 10, 64)
		if err != nil {
			return BoardResponse{}, err
		}
		manager.mu.Lock()
		if view.lastTop == nil && view.wantedBoard == 0 {
			view.sentBoard, view.lastTop = version, answer.Top
		}
		manager.mu.Unlock()
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
	manager.mu.Lock()
	view := manager.audiences[sessionID]
	manager.mu.Unlock()
	return manager.readBoard(ctx, view, strconv.FormatUint(roomID, 10), strconv.FormatUint(sessionID, 10), strconv.FormatUint(userID, 10))
}

func boardFrame(kind string, data any) ([]byte, error) {
	return json.Marshal(struct {
		Version int    `json:"version"`
		Type    string `json:"type"`
		Data    any    `json:"data"`
	}{1, kind, data})
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
	frame, err := boardFrame("LIKE_BOARD_DELTA", data)
	if err != nil || len(frame) <= 8*1024 {
		return frame, err
	}
	return boardFrame("LIKE_BOARD_REFRESH", map[string]string{"room_id": board.RoomID, "session_id": board.SessionID, "projection_epoch": board.Epoch, "revision": board.Revision})
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

func (manager *Manager) flushLeaderboard(ctx context.Context) {
	manager.mu.Lock()
	work := make(map[uint64]*audience)
	for sessionID, view := range manager.audiences {
		if !manager.available.Load() {
			break
		}
		for id, updates := range view.dirtyPeople {
			frame, err := boardFrame("MY_LIKES_UPDATE", map[string]string{
				"session_id": strconv.FormatUint(sessionID, 10), "projection_epoch": manager.epoch,
				"likes_revision": strconv.FormatInt(updates.revision, 10), "count": updates.count,
			})
			if err != nil {
				slog.Error("encode_personal_likes", "error", err)
				continue
			}
			for client := range updates.clients {
				client.enqueue(frame)
			}
			delete(view.dirtyPeople, id)
		}
		if view.wantedBoard > view.sentBoard {
			work[sessionID] = view
		}
	}
	manager.mu.Unlock()
	for sessionID, view := range work {
		if ctx.Err() != nil {
			return
		}
		read, cancel := context.WithTimeout(ctx, time.Second)
		board, err := manager.readBoard(read, view, strconv.FormatUint(view.roomID, 10), strconv.FormatUint(sessionID, 10), "")
		cancel()
		if err != nil {
			// 未推送的版本保留至下一轮重新读取。
			continue
		}
		version, err := strconv.ParseInt(board.Revision, 10, 64)
		if err != nil {
			continue
		}
		manager.mu.Lock()
		if manager.audiences[sessionID] == view && manager.available.Load() && version > view.sentBoard {
			frame, err := boardDelta(view.lastTop, strconv.FormatInt(view.sentBoard, 10), board)
			if err != nil {
				manager.mu.Unlock()
				slog.Error("encode_board_delta", "error", err)
				continue
			}
			for client := range view.clients {
				client.enqueue(frame)
			}
			view.lastTop, view.sentBoard = board.Top, version
		}
		manager.mu.Unlock()
	}
}

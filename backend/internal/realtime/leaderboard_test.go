package realtime

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
)

func TestBoardDeltaBudget(t *testing.T) {
	board := BoardResponse{RoomID: "1", SessionID: "2", Epoch: "e", Revision: "7"}
	for i := 1; i <= 100; i++ {
		board.Top = append(board.Top, BoardRow{Rank: i, Username: strings.Repeat("x", 32), Count: "9007199254740993", UserID: strconv.Itoa(i)})
	}
	check := func(before []BoardRow, kind string, changes int) {
		t.Helper()
		frame, err := boardDelta(before, "5", board)
		var message struct {
			Type string
			Data struct{ Changes []BoardRow }
		}
		if err != nil || len(frame) > 8192 || json.Unmarshal(frame, &message) != nil || message.Type != kind || len(message.Data.Changes) != changes {
			t.Fatalf("榜单更新错误 %s %d %v", message.Type, len(frame), err)
		}
	}
	check(nil, "LIKE_BOARD_REFRESH", 0)
	before := append([]BoardRow(nil), board.Top...)
	before[99].Count = "1"
	check(before, "LIKE_BOARD_DELTA", 1)
}

func TestBoardPersonalRouting(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "epoch")
	manager.SetAvailable(true)
	viewer := &connection{send: make(chan []byte, 4)}
	secondTab := &connection{send: make(chan []byte, 4)}
	other := &connection{send: make(chan []byte, 4)}
	view := &audience{people: map[string]*personalUpdate{"9": {clients: map[*connection]struct{}{viewer: {}, secondTab: {}}}, "10": {clients: map[*connection]struct{}{other: {}}}}, dirtyPeople: make(map[string]*personalUpdate)}
	manager.audiences[2] = view
	for _, rev := range []string{"12", "9"} {
		manager.QueueLikeUpdate(messaging.LikeEvent{SessionID: "2", UserID: "9"}, messaging.ProjectedLike{Revision: rev, Personal: rev, BoardRevision: "7"})
	}
	manager.flushLeaderboard(t.Context())
	manager.flushLeaderboard(t.Context())
	if len(viewer.send) != 1 || len(secondTab.send) != 1 || len(other.send) != 0 {
		t.Fatal("个人数据发送到错误的连接或重复发送")
	}
	var message struct {
		Type string
		Data struct{ Count string }
	}
	if err := json.Unmarshal(<-viewer.send, &message); err != nil || message.Type != "MY_LIKES_UPDATE" || message.Data.Count != "12" {
		t.Fatal(message, err)
	}
}

package realtime

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
)

func TestReadLikeCount(t *testing.T) {
	for input, want := range map[string]int64{`{}`: 1, `{"count":5}`: 5, `{"count":9223372036854775807}`: math.MaxInt64} {
		got, err := readLikeCount([]byte(input))
		if got != want || err != nil {
			t.Errorf("%s: %d %v", input, got, err)
		}
	}
	for _, input := range []string{`null`, `[]`, `{"count":null}`, `{"count":0}`, `{"count":-1}`, `{"count":1.5}`, `{"count":"8"}`, `{"count":2,"extra":1}`, `{"count":9223372036854775808}`} {
		if _, err := readLikeCount([]byte(input)); err == nil {
			t.Errorf("接受了非法数量 %s", input)
		}
	}
}

// 不同分区可以乱序完成，打开过的推送窗口也不能使 revision 回退。
func TestCoalescedLikeUpdates(t *testing.T) {
	manager := NewManager(nil, nil, nil, nil, nil, "epoch")
	manager.SetAvailable(true)
	viewer := &connection{send: make(chan []byte, 8)}
	other := &connection{send: make(chan []byte, 8)}
	manager.audiences[2] = &audience{roomID: 1, clients: map[*connection]struct{}{viewer: {}}}
	manager.audiences[3] = &audience{roomID: 1, clients: map[*connection]struct{}{other: {}}}
	event := messaging.LikeEvent{RoomID: "1", SessionID: "2"}
	for _, rev := range []string{"3", "15", "8"} {
		manager.QueueLikeUpdate(event, messaging.ProjectedLike{Count: rev, Revision: rev})
	}
	if len(viewer.send) != 0 {
		t.Fatal("尚未到窗口就发送了快照")
	}
	manager.flushLikeUpdates()
	manager.QueueLikeUpdate(event, messaging.ProjectedLike{Count: "9", Revision: "9"})
	manager.flushLikeUpdates()
	if len(viewer.send) != 1 || len(other.send) != 0 {
		t.Fatal("广播未按场次或窗口合并")
	}
	var frame struct{ Data likeUpdate }
	if err := json.Unmarshal(<-viewer.send, &frame); err != nil || frame.Data.Count != "15" {
		t.Fatalf("快照错误：%+v %v", frame, err)
	}
}

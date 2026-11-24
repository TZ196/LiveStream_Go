package realtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type observedFrame struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	EventID   string          `json:"event_id"`
	Data      json.RawMessage `json:"data"`
	at        time.Time
	err       error
}

type apiSocket struct {
	conn   *websocket.Conn
	frames chan observedFrame
}

// apiRequest 只操作显式指定的隔离 API，错误输出仅包含状态码，避免输出登录凭证。
func apiRequest(t *testing.T, method, path, token string, input, output any, status int) {
	t.Helper()
	var data []byte
	if input != nil {
		var err error
		data, err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, os.Getenv("LIVESTREAM_TEST_API")+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", os.Getenv("LIVESTREAM_WS_ORIGIN"))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		t.Fatalf("%s %s: status=%d, want=%d", method, path, response.StatusCode, status)
	}
	if output != nil {
		var envelope struct{ Data json.RawMessage }
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(envelope.Data, output); err != nil {
			t.Fatal(err)
		}
	}
}

func apiAccount(t *testing.T) string {
	t.Helper()
	credentials := map[string]string{"username": "r" + strconv.FormatInt(time.Now().UnixNano(), 10), "password": uuid.NewString()}
	apiRequest(t, "POST", "/api/v1/auth/register", "", credentials, nil, 201)
	var login struct {
		AccessToken string `json:"access_token"`
	}
	apiRequest(t, "POST", "/api/v1/auth/login", "", credentials, &login, 200)
	if login.AccessToken == "" {
		t.Fatal("登录缺少令牌")
	}
	return login.AccessToken
}

func apiConnect(t *testing.T, token, roomID, expectedCount string) (*apiSocket, string) {
	t.Helper()
	var ticket struct {
		Path      string `json:"ws_path"`
		SessionID string `json:"session_id"`
	}
	apiRequest(t, "POST", "/api/v1/rooms/"+roomID+"/ws-ticket", token, struct{}{}, &ticket, 201)
	address := strings.Replace(os.Getenv("LIVESTREAM_TEST_API"), "http", "ws", 1) + ticket.Path
	headers := http.Header{"Origin": []string{os.Getenv("LIVESTREAM_WS_ORIGIN")}}
	conn, _, err := websocket.DefaultDialer.Dial(address, headers)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var initial observedFrame
	if err := conn.ReadJSON(&initial); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	var state struct {
		Count     string `json:"like_count"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(initial.Data, &state); err != nil || initial.Type != "ROOM_STATE" || state.Count != expectedCount || state.SessionID != ticket.SessionID {
		conn.Close()
		t.Fatal("重连首帧或 Redis 权威首屏不符合预期")
	}
	_ = conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	socket := &apiSocket{conn: conn, frames: make(chan observedFrame, 512)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(socket.frames)
		for {
			var frame observedFrame
			frame.err = conn.ReadJSON(&frame)
			frame.at = time.Now()
			socket.frames <- frame
			if frame.err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		conn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("测试连接的读协程未退出")
		}
	})
	return socket, ticket.SessionID
}

func apiAwait(t *testing.T, socket *apiSocket, accept func(observedFrame) bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case frame, ok := <-socket.frames:
			if !ok || frame.err != nil {
				t.Fatal("连接在满足业务断言前关闭")
			}
			if accept(frame) {
				return
			}
		case <-deadline.C:
			t.Fatal("等待业务消息超时")
		}
	}
}

func apiLike(t *testing.T, socket *apiSocket, session, request string, count int) {
	t.Helper()
	err := socket.conn.WriteJSON(map[string]any{"version": 1, "type": "LIKE", "session_id": session, "request_id": request, "data": map[string]int{"count": count}})
	if err != nil {
		t.Fatal(err)
	}
}

// TestLikeAPIIntegration 经实际 HTTP 和 WebSocket 入口验证双组、重试、个人推送与重连。
// 测试前须在新建的隔离环境启动 API 和 Worker，并设置相应 TEST 环境变量。
func TestLikeAPIIntegration(t *testing.T) {
	dsn := os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")
	if os.Getenv("LIVESTREAM_TEST_API") == "" || dsn == "" {
		t.Skip("需要隔离 API、Worker 和测试数据库")
	}
	owner, spectator := apiAccount(t), apiAccount(t)
	var room struct{ ID string }
	apiRequest(t, "POST", "/api/v1/rooms", owner, map[string]string{"title": "重构回归"}, &room, 201)
	one, session := apiConnect(t, owner, room.ID, "0")
	two, _ := apiConnect(t, owner, room.ID, "0")
	other, _ := apiConnect(t, spectator, room.ID, "0")
	request := uuid.Must(uuid.NewV7()).String()
	apiLike(t, one, session, request, 5)
	apiLike(t, one, session, request, 5)
	acks, total, personal := 0, false, false
	var eventID string
	apiAwait(t, one, func(frame observedFrame) bool {
		if frame.Type == "ACK" && frame.RequestID == request {
			acks++
			if eventID != "" && eventID != frame.EventID {
				t.Fatal("重试的事件 ID 改变")
			}
			eventID = frame.EventID
		}
		var data map[string]any
		if err := json.Unmarshal(frame.Data, &data); err != nil {
			t.Fatal(err)
		}
		total = total || (frame.Type == "LIKE_UPDATE" && data["like_count"] == "5")
		personal = personal || (frame.Type == "MY_LIKES_UPDATE" && data["count"] == "5")
		return acks == 2 && total && personal
	})
	apiAwait(t, two, func(frame observedFrame) bool { return frame.Type == "MY_LIKES_UPDATE" })
	apiAwait(t, other, func(frame observedFrame) bool {
		if frame.Type == "MY_LIKES_UPDATE" {
			t.Fatal("个人点赞数发送给了其他用户")
		}
		return frame.Type == "LIKE_BOARD_DELTA"
	})
	one.conn.Close()
	reconnected, sameSession := apiConnect(t, owner, room.ID, "5")
	if sameSession != session {
		t.Fatal("重连意外改变场次")
	}
	// 同一负载同时用于重构前后测量。绝对计数快照覆盖的每个批次只登记一次送达。
	const batches = 100
	ackTimes := make([]time.Duration, 0, batches)
	deliveryTimes := make([]time.Duration, 0, batches)
	sent := make([]time.Time, 0, batches)
	delivered := 0
	observe := func(frame observedFrame) {
		if frame.Type != "LIKE_UPDATE" {
			return
		}
		var update struct {
			Count string `json:"like_count"`
		}
		if err := json.Unmarshal(frame.Data, &update); err != nil {
			t.Fatal(err)
		}
		count, err := strconv.Atoi(update.Count)
		if err != nil {
			t.Fatal(err)
		}
		through := (count - 5) / 3
		if through > len(sent) {
			t.Fatal("收到超出已发送数量的总数")
		}
		for delivered < through {
			deliveryTimes = append(deliveryTimes, frame.at.Sub(sent[delivered]))
			delivered++
		}
	}
	for i := 0; i < batches; i++ {
		id := uuid.Must(uuid.NewV7()).String()
		start := time.Now()
		sent = append(sent, start)
		apiLike(t, reconnected, session, id, 3)
		apiAwait(t, reconnected, func(frame observedFrame) bool {
			observe(frame)
			if frame.Type == "ACK" && frame.RequestID == id {
				ackTimes = append(ackTimes, frame.at.Sub(start))
				return true
			}
			return false
		})
		time.Sleep(20 * time.Millisecond)
	}
	apiAwait(t, reconnected, func(frame observedFrame) bool {
		observe(frame)
		return delivered == batches
	})
	var board BoardResponse
	apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/sessions/"+session+"/like-leaderboard", owner, nil, &board, 200)
	if board.Me.Count != "305" || len(board.Top) != 1 || board.Top[0].Count != "305" {
		t.Fatal("榜单和个人累计不一致")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var persisted struct{ Events, Clicks int64 }
		err = db.Table("interaction_events").Select("COUNT(*) AS events, COALESCE(SUM(click_count), 0) AS clicks").Where("session_id = ?", session).Scan(&persisted).Error
		if err != nil {
			t.Fatal(err)
		}
		if persisted.Events == 101 && persisted.Clicks == 305 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL 结果不一致: %+v", persisted)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, series := range []struct {
		name   string
		values []time.Duration
	}{{"ACK", ackTimes}, {"snapshot", deliveryTimes}} {
		sort.Slice(series.values, func(i, j int) bool { return series.values[i] < series.values[j] })
		t.Logf("%s samples=%d p95=%s p99=%s", series.name, len(series.values), series.values[94], series.values[98])
	}
	apiRequest(t, "POST", "/api/v1/rooms/"+room.ID+"/close", owner, struct{}{}, nil, 200)
	apiRequest(t, "POST", "/api/v1/rooms/"+room.ID+"/reopen", owner, struct{}{}, nil, 200)
	_, newSession := apiConnect(t, owner, room.ID, "0")
	if newSession == session {
		t.Fatal("重新开播复用了旧场次")
	}
	t.Log("same-session retry and reconnect, 101 unique events, 305 clicks, fresh session verified")
}

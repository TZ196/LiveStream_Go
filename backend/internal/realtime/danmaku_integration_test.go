package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestDanmakuAPIIntegration 仅操作显式指定的隔离环境；各模式须分别使用全新库、Topic、Redis。
func TestDanmakuAPIIntegration(t *testing.T) {
	mode := os.Getenv("LIVESTREAM_TEST_DANMAKU_API")
	if mode == "" || os.Getenv("LIVESTREAM_TEST_API") == "" {
		t.Skip("需要隔离 API/Worker 和显式弹幕测试模式")
	}
	db, err := gorm.Open(mysql.Open(os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("连接隔离数据库失败")
	}
	pool, _ := db.DB()
	defer pool.Close()
	token := apiAccount(t)
	var room struct{ ID string }
	apiRequest(t, "POST", "/api/v1/rooms", token, map[string]string{"title": "弹幕完整链路 " + mode}, &room, 201)
	wantCount := "0"
	if mode == "likes-disabled" {
		wantCount = ""
	}
	socket, session := apiConnect(t, token, room.ID, wantCount)
	var capability struct {
		Likes   bool `json:"likes_available"`
		Danmaku bool `json:"danmaku_available"`
	}
	if json.Unmarshal(socket.initial.Data, &capability) != nil || capability.Danmaku != (mode != "words-missing") || capability.Likes != (mode != "likes-disabled") {
		t.Fatal("首屏业务能力未独立反映实际状态")
	}
	countDanmaku := func() int64 {
		var count int64
		if err := db.Table("danmaku_messages").Where("room_id=?", room.ID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		return count
	}
	countLikes := func(session string) int64 {
		var count int64
		if err := db.Table("interaction_events").Where("session_id=?", session).Select("COALESCE(SUM(click_count),0)").Scan(&count).Error; err != nil {
			t.Fatal(err)
		}
		return count
	}
	if mode == "words-missing" {
		id := uuid.Must(uuid.NewV7()).String()
		apiDanmaku(t, socket, session, id, "词库故障时禁止发送")
		apiErrorCode(t, socket, id, "DANMAKU_UNAVAILABLE")
		apiLike(t, socket, session, uuid.Must(uuid.NewV7()).String(), 3)
		apiAwait(t, socket, func(frame observedFrame) bool { return frame.Type == "ACK" })
		danmakuWait(t, func() bool { return countLikes(session) == 3 })
		if countDanmaku() != 0 {
			t.Fatal("词库损坏仍接受了新弹幕")
		}
		assertAPIReady(t)
		return
	}
	firstID := uuid.Must(uuid.NewV7()).String()
	apiDanmaku(t, socket, session, firstID, "第一条正常弹幕")
	firstEvent := danmakuAcceptedAndSeen(t, socket, firstID)
	danmakuWait(t, func() bool { return countDanmaku() == 1 })
	if mode == "likes-disabled" {
		assertAPIReady(t)
		t.Log("点赞未启用时，票据、握手、弹幕接受、广播和持久化均正常")
		return
	}
	if mode == "like-poison" {
		injectInvalidEvent(t, "live.events")
		danmakuWait(t, func() bool {
			var state struct{ State string }
			apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/likes-state", token, nil, &state, 200)
			return state.State == "BLOCKED"
		})
		next := uuid.Must(uuid.NewV7()).String()
		apiDanmaku(t, socket, session, next, "点赞故障后同连接继续弹幕")
		danmakuAcceptedAndSeen(t, socket, next)
		fresh, _ := apiConnect(t, token, room.ID, "")
		next = uuid.Must(uuid.NewV7()).String()
		apiDanmaku(t, fresh, session, next, "点赞故障后新连接继续弹幕")
		danmakuAcceptedAndSeen(t, fresh, next)
		danmakuWait(t, func() bool { return countDanmaku() == 3 })
		assertAPIReady(t)
		t.Log("点赞 BLOCKED 后，旧连接和新连接的弹幕仍能广播并落库")
		return
	}
	if mode != "normal" {
		t.Fatal("未知隔离测试模式")
	}
	otherToken := apiAccount(t)
	spectator, _ := apiConnect(t, otherToken, room.ID, "0")
	var otherRoom struct{ ID string }
	apiRequest(t, "POST", "/api/v1/rooms", otherToken, map[string]string{"title": "隔离房间"}, &otherRoom, 201)
	otherSocket, _ := apiConnect(t, otherToken, otherRoom.ID, "0")
	for index := 0; index < 3; index++ {
		apiDanmaku(t, socket, session, firstID, "第一条正常弹幕")
		apiAwait(t, socket, func(frame observedFrame) bool {
			if frame.Type == "DANMAKU_BROADCAST" {
				t.Fatal("已接受请求重试重复广播")
			}
			return frame.Type == "ACK" && frame.EventID == firstEvent
		})
	}
	apiDanmaku(t, socket, session, firstID, "同 ID 改变内容")
	apiErrorCode(t, socket, firstID, "REQUEST_CONFLICT")
	blocked := uuid.Must(uuid.NewV7()).String()
	apiDanmaku(t, socket, session, blocked, "刷单垫付")
	apiErrorCode(t, socket, blocked, "CONTENT_BLOCKED")
	if countDanmaku() != 1 {
		t.Fatal("拒绝或重试增加了持久化消息")
	}

	// 单用户依次发送，不要求不同用户按墙钟时间排序；所有连接仍看到同一分发顺序。
	var sends sync.WaitGroup
	for index, target := range []*apiSocket{socket, spectator} {
		sends.Add(1)
		go func(index int, target *apiSocket) {
			defer sends.Done()
			for n := 0; n < 5; n++ {
				_ = target.conn.WriteJSON(map[string]any{"version": 1, "type": "DANMAKU", "session_id": session, "request_id": uuid.Must(uuid.NewV7()).String(), "data": map[string]string{"content": fmt.Sprintf("并发用户%d消息%d", index, n)}})
			}
		}(index, target)
	}
	sends.Wait()
	collect := func(target *apiSocket) []string {
		items := []string{}
		last := map[int]int{0: 0, 1: 0}
		apiAwait(t, target, func(frame observedFrame) bool {
			if frame.Type != "DANMAKU_BROADCAST" {
				return false
			}
			var value struct{ Content string }
			if json.Unmarshal(frame.Data, &value) != nil {
				t.Fatal("广播格式错误")
			}
			var user, index int
			if _, err := fmt.Sscanf(value.Content, "并发用户%d消息%d", &user, &index); err != nil || last[user] != index {
				t.Fatal("同用户消息顺序发生改变")
			}
			last[user]++
			items = append(items, frame.EventID)
			return len(items) == 10
		})
		return items
	}
	if left, right := collect(socket), collect(spectator); !reflect.DeepEqual(left, right) {
		t.Fatal("同场次客户端分发顺序不同")
	}
	select {
	case frame := <-otherSocket.frames:
		if frame.Type == "DANMAKU_BROADCAST" {
			t.Fatal("弹幕广播到了其他房间")
		}
	default:
	}
	danmakuWait(t, func() bool { return countDanmaku() == 11 })
	var history danmaku.History
	path := "/api/v1/rooms/" + room.ID + "/danmaku?session_id=" + session + "&limit=5"
	apiRequest(t, "GET", path, token, nil, &history, 200)
	if len(history.Items) != 5 || history.NextCursor == nil || history.Consistency != "eventual" {
		t.Fatal("历史分页不符合约定")
	}
	for index := 1; index < len(history.Items); index++ {
		if history.Items[index].ID >= history.Items[index-1].ID {
			t.Fatal("历史未按数据库 ID 倒序")
		}
	}
	apiRequest(t, "GET", path+"&cursor="+url.QueryEscape(*history.NextCursor), token, nil, &history, 200)
	if len(history.Items) != 5 || history.NextCursor == nil {
		t.Fatal("历史第二页缺少数据")
	}
	apiRequest(t, "GET", "/api/v1/rooms/"+otherRoom.ID+"/danmaku?cursor="+url.QueryEscape(*history.NextCursor), token, nil, nil, 400)
	apiRequest(t, "GET", path+"&limit=2", token, nil, nil, 400)
	apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/danmaku?session_id=", token, nil, nil, 400)
	apiLike(t, socket, session, uuid.Must(uuid.NewV7()).String(), 3)
	apiAwait(t, socket, func(frame observedFrame) bool { return frame.Type == "ACK" })
	danmakuWait(t, func() bool { return countLikes(session) == 3 })

	// 本轮全新测试库中的触发器注入事务失败；检查点不得越过失败消息。
	checkpoint := func(group string) int64 {
		var value int64
		if err := db.Table("worker_checkpoints").Select("COALESCE(SUM(next_offset),0)").Where("consumer_group=?", group).Scan(&value).Error; err != nil {
			t.Fatal(err)
		}
		return value
	}
	execSQL := func(query string) {
		if err := db.Exec(query).Error; err != nil {
			t.Fatal("隔离事务故障设置失败")
		}
	}
	before := checkpoint(danmaku.PersistenceGroup)
	execSQL("CREATE TRIGGER test_fail_danmaku BEFORE INSERT ON danmaku_messages FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated danmaku failure'")
	defer db.Exec("DROP TRIGGER IF EXISTS test_fail_danmaku")
	id := uuid.Must(uuid.NewV7()).String()
	apiDanmaku(t, socket, session, id, "弹幕事务失败仍在线广播")
	danmakuAcceptedAndSeen(t, socket, id)
	apiLike(t, socket, session, uuid.Must(uuid.NewV7()).String(), 2)
	apiAwait(t, socket, func(frame observedFrame) bool { return frame.Type == "ACK" })
	danmakuWait(t, func() bool { return countLikes(session) == 5 })
	time.Sleep(300 * time.Millisecond)
	if countDanmaku() != 11 || checkpoint(danmaku.PersistenceGroup) != before {
		t.Fatal("弹幕事务失败仍写入明细或推进检查点")
	}
	execSQL("DROP TRIGGER test_fail_danmaku")
	danmakuWait(t, func() bool { return countDanmaku() == 12 })

	before = checkpoint("live-persistence")
	execSQL("CREATE TRIGGER test_fail_like BEFORE INSERT ON interaction_events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='isolated like failure'")
	defer db.Exec("DROP TRIGGER IF EXISTS test_fail_like")
	apiLike(t, socket, session, uuid.Must(uuid.NewV7()).String(), 1)
	apiAwait(t, socket, func(frame observedFrame) bool { return frame.Type == "ACK" })
	id = uuid.Must(uuid.NewV7()).String()
	apiDanmaku(t, socket, session, id, "点赞事务失败不影响弹幕持久化")
	danmakuAcceptedAndSeen(t, socket, id)
	danmakuWait(t, func() bool { return countDanmaku() == 13 })
	if countLikes(session) != 5 || checkpoint("live-persistence") != before {
		t.Fatal("点赞事务失败仍改变业务或检查点")
	}
	execSQL("DROP TRIGGER test_fail_like")
	danmakuWait(t, func() bool { return countLikes(session) == 6 })

	var oldMessage danmaku.Message
	if err := db.Where("event_id=?", firstEvent).Take(&oldMessage).Error; err != nil {
		t.Fatal(err)
	}
	apiRequest(t, "POST", "/api/v1/rooms/"+room.ID+"/close", token, struct{}{}, nil, 200)
	apiRequest(t, "POST", "/api/v1/rooms/"+room.ID+"/reopen", token, struct{}{}, nil, 200)
	fresh, nextSession := apiConnect(t, token, room.ID, "0")
	if nextSession == session {
		t.Fatal("重新开播复用了旧场次")
	}
	id = uuid.Must(uuid.NewV7()).String()
	apiDanmaku(t, fresh, nextSession, id, "新场次弹幕")
	danmakuAcceptedAndSeen(t, fresh, id)
	producer, err := danmaku.NewProducer([]string{os.Getenv("LIVESTREAM_TEST_KAFKA")})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Publish(context.Background(), oldMessage.Event()); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
observeOld:
	for {
		select {
		case frame := <-fresh.frames:
			if frame.Type == "DANMAKU_BROADCAST" {
				t.Fatal("旧场次重放进入新场次")
			}
		case <-timer.C:
			break observeOld
		}
	}
	// 异常消息作为本模式最后一步：弹幕消费组暂停后点赞必须继续。
	injectInvalidEvent(t, danmaku.Topic)
	apiAwait(t, fresh, func(frame observedFrame) bool {
		var data struct {
			Danmaku bool `json:"danmaku_available"`
		}
		return frame.Type == "ROOM_STATE" && json.Unmarshal(frame.Data, &data) == nil && !data.Danmaku
	})
	apiLike(t, fresh, nextSession, uuid.Must(uuid.NewV7()).String(), 4)
	apiAwait(t, fresh, func(frame observedFrame) bool { return frame.Type == "ACK" })
	danmakuWait(t, func() bool { return countLikes(nextSession) == 4 })
	assertAPIReady(t)
	t.Log("双业务、幂等、审核、顺序、历史、独立事务失败、跨场次和消费故障隔离通过")
}

func apiDanmaku(t *testing.T, socket *apiSocket, session, request, content string) {
	t.Helper()
	if err := socket.conn.WriteJSON(map[string]any{"version": 1, "type": "DANMAKU", "session_id": session, "request_id": request, "data": map[string]string{"content": content}}); err != nil {
		t.Fatal(err)
	}
}
func danmakuAcceptedAndSeen(t *testing.T, socket *apiSocket, request string) string {
	t.Helper()
	accepted, seen, event := false, false, ""
	apiAwait(t, socket, func(frame observedFrame) bool {
		if frame.RequestID != request {
			return false
		}
		if frame.Type == "ERROR" {
			var data struct{ Code string }
			_ = json.Unmarshal(frame.Data, &data)
			t.Fatalf("合法弹幕被拒绝: %s", data.Code)
		}
		if frame.Type == "ACK" {
			accepted = true
		}
		if frame.Type == "DANMAKU_BROADCAST" {
			seen = true
		}
		if frame.EventID != "" {
			if event != "" && frame.EventID != event {
				t.Fatal("ACK 与广播身份不同")
			}
			event = frame.EventID
		}
		return accepted && seen
	})
	return event
}
func apiErrorCode(t *testing.T, socket *apiSocket, request, code string) {
	t.Helper()
	apiAwait(t, socket, func(frame observedFrame) bool {
		if frame.Type != "ERROR" || frame.RequestID != request {
			return false
		}
		var data struct{ Code string }
		if json.Unmarshal(frame.Data, &data) != nil || data.Code != code {
			t.Fatalf("拒绝原因错误: %s, want=%s", data.Code, code)
		}
		return true
	})
}
func danmakuWait(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("等待隔离业务状态超时")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func injectInvalidEvent(t *testing.T, topic string) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("LIVESTREAM_TEST_KAFKA")))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("isolated-invalid"), Value: []byte("{}")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
func assertAPIReady(t *testing.T) {
	t.Helper()
	client := http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(strings.TrimRight(os.Getenv("LIVESTREAM_TEST_API"), "/") + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("业务故障错误地关闭了普通 API 就绪状态")
	}
}

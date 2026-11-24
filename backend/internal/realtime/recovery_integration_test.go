package realtime

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestAutomaticRedisRecoveryIntegration 由隔离控制进程重启 Redis、还原旧 AOF 和停启 Worker。
func TestAutomaticRedisRecoveryIntegration(t *testing.T) {
	directory := os.Getenv("LIVESTREAM_TEST_CONTROL_DIR")
	if directory == "" || os.Getenv("LIVESTREAM_TEST_AUTOMATIC_RECOVERY") != "1" {
		t.Skip("需要隔离 API 和故障控制进程")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := gorm.Open(mysql.Open(os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("连接隔离数据库失败")
	}
	pool, _ := db.DB()
	defer pool.Close()
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("LIVESTREAM_TEST_REDIS")})
	defer client.Close()
	admin, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("LIVESTREAM_TEST_KAFKA")))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	token := apiAccount(t)
	var room struct{ ID string }
	apiRequest(t, "POST", "/api/v1/rooms", token, map[string]string{"title": "自动恢复验证"}, &room, 201)
	socket, session := apiConnect(t, token, room.ID, "0")
	activeEpoch := func() string {
		var epoch string
		if err := db.Table("projection_state").Where("id=1").Pluck("epoch", &epoch).Error; err != nil {
			t.Fatal(err)
		}
		return epoch
	}
	original := activeEpoch()
	waitFor := func(check func() bool) {
		deadline := time.Now().Add(35 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("等待自动恢复超时")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitSQL := func(want int64) {
		waitFor(func() bool {
			var total int64
			db.Table("interaction_events").Select("COALESCE(SUM(click_count),0)").Where("session_id=?", session).Scan(&total)
			return total == want
		})
	}
	waitReady := func(count, previous string, changed bool) {
		waitFor(func() bool {
			var state struct{ State string }
			apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/likes-state", token, nil, &state, 200)
			epoch := activeEpoch()
			if state.State != "READY" || (changed && epoch == previous) || (!changed && epoch != previous) {
				return false
			}
			info, err := client.Info(ctx, "server").Result()
			if err != nil {
				return false
			}
			run := ""
			for _, line := range strings.Split(info, "\n") {
				if strings.HasPrefix(line, "run_id:") {
					run = strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
				}
			}
			identity, err := client.Get(ctx, "ls:v1:projection:"+epoch+":identity").Result()
			if err != nil || !strings.HasSuffix(identity, ":"+run) {
				return false
			}
			current, err := client.HGet(ctx, "ls:v1:projection:"+epoch+":session:"+session, "likes").Result()
			return err == nil && current == count
		})
	}
	send := func(sock *apiSocket, id string, count int) {
		apiLike(t, sock, session, id, count)
		apiAwait(t, sock, func(frame observedFrame) bool { return frame.Type == "ACK" && frame.RequestID == id })
	}
	progress := func() int64 {
		request := kmsg.NewPtrOffsetFetchRequest()
		request.Group = "live-realtime"
		var partitions []int32
		for part := int32(0); part < 16; part++ {
			partitions = append(partitions, part)
		}
		request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "live.events", Partitions: partitions}}
		request.Groups = []kmsg.OffsetFetchRequestGroup{{Group: "live-realtime", Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "live.events", Partitions: partitions}}}}
		response, err := request.RequestWith(ctx, admin)
		if err != nil || len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 {
			t.Fatal("只读检查消费位移失败")
		}
		var sum int64
		for _, topic := range response.Groups[0].Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != 0 {
					t.Fatal("读取分区位移失败")
				}
				sum += max(partition.Offset, 0)
			}
		}
		return sum
	}

	expectDuplicate := func(sock *apiSocket, id string, count int, total string) {
		before := progress()
		send(sock, id, count)
		waitFor(func() bool { return progress() > before })
		var board BoardResponse
		apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/sessions/"+session+"/like-leaderboard", token, nil, &board, 200)
		if board.Me.Count != total {
			t.Fatalf("恢复后重试重复计数: %s", board.Me.Count)
		}
	}
	send(socket, uuid.Must(uuid.NewV7()).String(), 7)
	waitSQL(7)
	waitReady("7", original, false)
	var initial struct {
		Generation string `json:"projection_generation"`
	}
	if err := json.Unmarshal(socket.initial.Data, &initial); err != nil {
		t.Fatal(err)
	}
	recoveryStep(t, directory, "capture-aof", "redis-restarted")
	waitReady("7", original, false)
	// 正常重启沿用同一 epoch，但共享原连接仍必须收到新的能力快照。
	apiAwait(t, socket, func(frame observedFrame) bool {
		var state struct {
			Generation string `json:"projection_generation"`
			Available  bool   `json:"likes_available"`
		}
		if frame.Type != "ROOM_STATE" || json.Unmarshal(frame.Data, &state) != nil {
			return false
		}
		before, _ := strconv.ParseUint(initial.Generation, 10, 64)
		after, _ := strconv.ParseUint(state.Generation, 10, 64)
		return state.Available && after > before
	})
	second := uuid.Must(uuid.NewV7()).String()
	send(socket, second, 5)
	waitSQL(12)
	waitFor(func() bool {
		var board BoardResponse
		apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/sessions/"+session+"/like-leaderboard", token, nil, &board, 200)
		return board.Me.Count == "12"
	})
	recoveryStep(t, directory, "rollback-aof", "aof-restored")
	waitReady("12", original, true)
	rebuiltEpoch := activeEpoch()
	socket, _ = apiConnect(t, token, room.ID, "12")
	expectDuplicate(socket, second, 5, "12")
	send(socket, uuid.Must(uuid.NewV7()).String(), 3)
	waitSQL(15)
	recoveryStep(t, directory, "stop-worker", "worker-stopped")
	backlog := uuid.Must(uuid.NewV7()).String()
	send(socket, backlog, 4)
	// Worker 已停，接收 ACK 仍只表示进入 Kafka。
	var persisted int64
	db.Table("interaction_events").Select("COALESCE(SUM(click_count),0)").Where("session_id=?", session).Scan(&persisted)
	if persisted != 15 {
		t.Fatal("Worker 停止期间数据库被错误地视为已追平")
	}
	recoveryStep(t, directory, "lose-redis", "redis-cleared")
	waitFor(func() bool {
		var state struct{ State string }
		apiRequest(t, "GET", "/api/v1/rooms/"+room.ID+"/likes-state", token, nil, &state, 200)
		return state.State == "RECOVERING"
	})
	// 点赞恢复不影响普通 HTTP 和共享握手；未知计数明确返回不可用。
	apiRequest(t, "GET", "/readyz", "", nil, nil, 200)
	apiRequest(t, "GET", "/api/v1/rooms/"+room.ID, token, nil, nil, 200)
	if apiAccount(t) == "" {
		t.Fatal("恢复期间不能注册登录")
	}
	duringRecovery, _ := apiConnect(t, token, room.ID, "")
	recoveryStep(t, directory, "waiting-for-worker", "worker-resumed")
	waitReady("19", rebuiltEpoch, true)
	apiAwait(t, duringRecovery, func(frame observedFrame) bool {
		var data struct {
			Available bool   `json:"likes_available"`
			Count     string `json:"like_count"`
		}
		return frame.Type == "ROOM_STATE" && json.Unmarshal(frame.Data, &data) == nil && data.Available && data.Count == "19"
	})
	socket = duringRecovery
	expectDuplicate(socket, backlog, 4, "19")
	send(socket, uuid.Must(uuid.NewV7()).String(), 2)
	waitSQL(21)
	var count int64
	db.Table("interaction_events").Where("session_id=?", session).Count(&count)
	if count != 5 {
		t.Fatalf("唯一批次数不正确: %s", strconv.FormatInt(count, 10))
	}
	t.Log("real Redis restart, older AOF rollback, complete Redis loss, Worker backlog, independent HTTP and idempotent reconnect verified; 5 events / 21 clicks")
}

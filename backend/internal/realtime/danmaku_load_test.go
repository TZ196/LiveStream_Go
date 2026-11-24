package realtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestDanmakuLoadIntegration 是短时可复现负载检查，不是容量承诺。
// 仅在隔离库直接建立无登录密码的测试用户，避免把注册限流混入弹幕吞吐口径。
func TestDanmakuLoadIntegration(t *testing.T) {
	if os.Getenv("LIVESTREAM_TEST_DANMAKU_LOAD") != "1" {
		t.Skip("需要隔离 API、Worker 和显式负载开关")
	}
	db, err := gorm.Open(mysql.Open(os.Getenv("LIVESTREAM_TEST_MYSQL_DSN")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("连接隔离库失败")
	}
	pool, _ := db.DB()
	defer pool.Close()
	secret, err := base64.StdEncoding.DecodeString(os.Getenv("LIVESTREAM_JWT_SECRET"))
	if err != nil || len(secret) < 32 {
		t.Fatal("需要与隔离 API 相同的测试签名密钥")
	}
	issuer := user.NewTokens(secret)
	const clients, perClient = 30, 4
	tokens := make([]string, clients)
	seed := fmt.Sprint(time.Now().UnixNano())
	for index := 0; index < clients; index++ {
		account := user.User{Username: fmt.Sprintf("load%s_%d", seed, index), PasswordHash: "load-only-no-valid-password", CreatedAt: time.Now().UTC()}
		if err := db.Create(&account).Error; err != nil {
			t.Fatal("建立隔离负载用户失败")
		}
		tokens[index], err = issuer.Issue(account.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	var hot, cold struct{ ID string }
	apiRequest(t, "POST", "/api/v1/rooms", tokens[0], map[string]string{"title": "负载热门房间"}, &hot, 201)
	apiRequest(t, "POST", "/api/v1/rooms", tokens[0], map[string]string{"title": "负载独立房间"}, &cold, 201)
	sockets := make([]*apiSocket, clients)
	sessions := make([]string, clients)
	starts := make(map[string]time.Time, clients*perClient)
	var mu sync.Mutex
	for index := 0; index < clients; index++ {
		target := hot.ID
		if index >= 24 {
			target = cold.ID
		}
		sockets[index], sessions[index] = apiConnect(t, tokens[index], target, "0")
	}
	ackTimes := make([]time.Duration, 0, clients*perClient)
	broadcastTimes := make([]time.Duration, 0, clients*perClient)
	failures := make(chan string, clients)
	var workers sync.WaitGroup
	started := time.Now()
	for index, socket := range sockets {
		workers.Add(1)
		go func(index int, socket *apiSocket) {
			defer workers.Done()
			own := make(map[string]bool, perClient)
			for n := 0; n < perClient; n++ {
				id := uuid.Must(uuid.NewV7()).String()
				own[id] = true
				mu.Lock()
				starts[id] = time.Now()
				mu.Unlock()
				if err := socket.conn.WriteJSON(map[string]any{"version": 1, "type": "DANMAKU", "session_id": sessions[index], "request_id": id, "data": map[string]string{"content": fmt.Sprintf("短时负载用户%d消息%d", index, n)}}); err != nil {
					failures <- "发送失败"
					return
				}
			}
			acks, seen := map[string]bool{}, map[string]bool{}
			timer := time.NewTimer(25 * time.Second)
			defer timer.Stop()
			for len(acks) < perClient || len(seen) < perClient {
				select {
				case frame, ok := <-socket.frames:
					if !ok || frame.err != nil {
						failures <- "负载期间连接关闭"
						return
					}
					if frame.Type == "ERROR" {
						failures <- "负载消息被拒绝"
						return
					}
					if frame.Type != "ACK" && frame.Type != "DANMAKU_BROADCAST" {
						continue
					}
					if !own[frame.RequestID] {
						continue
					}
					mu.Lock()
					elapsed := frame.at.Sub(starts[frame.RequestID])
					if frame.Type == "ACK" && !acks[frame.RequestID] {
						ackTimes = append(ackTimes, elapsed)
						acks[frame.RequestID] = true
					}
					if frame.Type == "DANMAKU_BROADCAST" && !seen[frame.RequestID] {
						broadcastTimes = append(broadcastTimes, elapsed)
						seen[frame.RequestID] = true
					}
					mu.Unlock()
				case <-timer.C:
					failures <- "负载消息确认超时"
					return
				}
			}
		}(index, socket)
	}
	workers.Wait()
	elapsed := time.Since(started)
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	danmakuWait(t, func() bool {
		var count int64
		if err := db.Table("danmaku_messages").Where("room_id IN ?", []string{hot.ID, cold.ID}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		return count == clients*perClient
	})
	// 独立只读拉取各分区，不创建第五个消费组，也不改动业务组位移。
	parts := make(map[int32]kgo.Offset)
	for partition := int32(0); partition < 16; partition++ {
		parts[partition] = kgo.NewOffset().AtStart()
	}
	observer, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("LIVESTREAM_TEST_KAFKA")), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{danmaku.Topic: parts}))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	observeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	counts, records := map[int]int{}, map[string]bool{}
	for len(records) < clients*perClient {
		fetches := observer.PollRecords(observeCtx, 200)
		if fetches.Err() != nil {
			t.Fatal("读取负载日志失败")
		}
		fetches.EachRecord(func(raw *kgo.Record) {
			record, decodeErr := danmaku.DecodeRecord(raw, "load-observer")
			if decodeErr != nil {
				t.Error(decodeErr)
				return
			}
			if record.Event.RoomID != hot.ID && record.Event.RoomID != cold.ID {
				return
			}
			if int(raw.Partition) != javaPartition(raw.Key, 16) {
				t.Error("实际 Kafka 分区不符合 Java murmur2")
			}
			if !records[record.Event.EventID] && record.Event.RoomID == hot.ID {
				counts[int(raw.Partition)]++
			}
			records[record.Event.EventID] = true
		})
	}
	if len(counts) < 8 {
		t.Fatal("热门房间未覆盖足够多的实际 Kafka 分区")
	}
	percentile := func(values []time.Duration, q float64) time.Duration {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return values[int(float64(len(values)-1)*q)]
	}
	t.Logf("clients=%d total=%d hot=96 cold=24 duration=%s confirmed_per_second=%.2f hot_key_partitions=%d distribution=%v", clients, clients*perClient, elapsed, float64(clients*perClient)/elapsed.Seconds(), len(counts), counts)
	t.Logf("Go WS client ACK p50=%s p95=%s p99=%s; receive p50=%s p95=%s p99=%s (not browser)", percentile(ackTimes, .5), percentile(ackTimes, .95), percentile(ackTimes, .99), percentile(broadcastTimes, .5), percentile(broadcastTimes, .95), percentile(broadcastTimes, .99))
}

// javaPartition 与 Kafka Java 默认 key 哈希一致；仅用于测试核对确定的分区键分布。
func javaPartition(data []byte, partitions int) int {
	const m uint32 = 0x5bd1e995
	h := uint32(0x9747b28c) ^ uint32(len(data))
	for len(data) >= 4 {
		k := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		k *= m
		k ^= k >> 24
		k *= m
		h *= m
		h ^= k
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int(h&0x7fffffff) % partitions
}

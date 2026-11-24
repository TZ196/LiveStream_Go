package likes

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/migrations"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestRecoveryIntegration 只运行于专门创建的空库和独立 Topic，不接触开发环境。
func TestRecoveryIntegration(t *testing.T) {
	if os.Getenv("LIVESTREAM_TEST_RECOVERY") != "1" {
		t.Skip("需要显式启用隔离恢复测试")
	}
	broker, dsn, address := os.Getenv("LIVESTREAM_TEST_KAFKA"), os.Getenv("LIVESTREAM_TEST_MYSQL_DSN"), os.Getenv("LIVESTREAM_TEST_REDIS")
	if broker == "" || dsn == "" || address == "" {
		t.Fatal("缺少隔离环境")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("连接隔离 MySQL 失败")
	}
	pool, _ := db.DB()
	defer pool.Close()
	if err := migrations.Run(ctx, db, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	var existing int64
	if err := db.Table("interaction_events").Count(&existing).Error; err != nil || existing != 0 {
		t.Fatal("恢复测试要求空业务库")
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	admin, err := newKafkaClient(ctx, []string{broker}, "live-realtime")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ends, err := admin.logOffsets(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	for _, pos := range ends {
		if pos != 0 {
			t.Fatal("恢复测试要求空 Topic")
		}
	}
	if err := admin.resetOffsets(ctx, ends); err != nil {
		t.Fatal(err)
	}
	epoch := uuid.NewString()
	recovery := NewRecovery(db, client, []string{broker}, epoch)
	projection, initializer, err := recovery.Check(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"INSERT INTO users (id,username,password_hash) VALUES (1,'recovery_a','test'),(2,'recovery_b','test')",
		"INSERT INTO live_rooms (id,owner_id,title,status) VALUES (1,1,'恢复场次','OPEN'),(2,1,'零赞场次','OPEN')",
		"INSERT INTO live_sessions (id,room_id) VALUES (1,1),(2,2)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initializer.EnsureSession(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := initializer.EnsureSession(ctx, 2, 2); err != nil {
		t.Fatal(err)
	}
	producer, err := NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	persistence := NewPersistenceRepository(db)
	var worker *Consumer
	var stopWorker context.CancelFunc
	var workerDone chan error
	startWorker := func() {
		var err error
		worker, err = NewConsumer(ctx, []string{broker}, "live-persistence")
		if err != nil {
			t.Fatal(err)
		}
		workerCtx, stop := context.WithCancel(ctx)
		stopWorker = stop
		workerDone = make(chan error, 1)
		go func() { workerDone <- worker.Run(workerCtx, persistence.PersistLike) }()
	}
	finishWorker := func() { stopWorker(); <-workerDone; worker.Close() }
	workerRunning := false
	defer func() {
		if workerRunning {
			finishWorker()
		}
	}()
	publish := func(event LikeEvent) ConsumedLike {
		key, value, err := event.KafkaRecord()
		if err != nil {
			t.Fatal(err)
		}
		raw := &kgo.Record{Topic: "live.events", Key: key, Value: value, Timestamp: time.Now()}
		if err := producer.client.ProduceSync(ctx, raw).FirstErr(); err != nil {
			t.Fatal(err)
		}
		record, err := DecodeLikeRecord(raw, admin.topicID)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	waitFor := func(check func() bool) {
		deadline := time.Now().Add(15 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("等待隔离链路超时")
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
	startWorker()
	workerRunning = true
	first, _ := NewLikeBatch("1", "1", "1", uuid.Must(uuid.NewV7()).String(), 5, time.Now())
	second, _ := NewLikeBatch("2", "1", "1", uuid.Must(uuid.NewV7()).String(), 7, time.Now())
	var records []ConsumedLike
	for _, event := range []LikeEvent{first, second, first} {
		record := publish(event)
		records = append(records, record)
		if _, err := projection.ApplyLike(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(func() bool { var n int64; db.Table("interaction_events").Count(&n); return n == 2 })
	t.Run("normal_restart_and_redis_ahead", func(t *testing.T) {
		// 模拟 AOF 恢复后保留旧运行编号，所有计数和去重依据仍完整。
		if err := client.Set(ctx, "ls:v1:projection:"+epoch+":identity", admin.topicID+":previous-run", 0).Err(); err != nil {
			t.Fatal(err)
		}
		next, _, err := recovery.Check(ctx)
		if err != nil || next.epoch != epoch {
			t.Fatalf("正常数据未沿用: %v", err)
		}
		state, err := next.ReadSession(ctx, "1", "1")
		if err != nil || state.Count != "12" {
			t.Fatalf("重复消费导致计数错误: %+v %v", state, err)
		}
		// 原 Lua 实例编号在运行期失效时，写入必须先被拒绝。
		stale, _ := NewProjectionRepository(client, epoch)
		stale.runID = "stale"
		if _, err := stale.ApplyLike(ctx, records[0]); !errors.Is(err, ErrProjectionRecovery) {
			t.Fatalf("未拒绝旧实例: %v", err)
		}
	})
	t.Run("ahead_requires_dedup", func(t *testing.T) {
		key := "ls:v1:projection:" + epoch + ":processed:" + first.EventID
		client.Del(ctx, key)
		if _, _, err := recovery.Check(ctx); !errors.Is(err, ErrProjectionRecovery) {
			t.Fatalf("缺少去重依据仍允许重放: %v", err)
		}
		client.Set(ctx, key, first.UserID+":"+first.RequestHash, 9*24*time.Hour)
	})
	finishWorker()
	workerRunning = false
	backlog, _ := NewLikeBatch("1", "1", "1", uuid.Must(uuid.NewV7()).String(), 11, time.Now())
	publish(backlog)
	// 仅删除本测试持有的原代次，模拟 Redis 丢失，而 Kafka/MySQL 保持。
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, "ls:v1:projection:"+epoch+":*", 256).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	type outcome struct {
		projection  *ProjectionRepository
		initializer *ProjectionInitializer
		err         error
	}
	rebuilt := make(chan outcome, 1)
	go func() { p, i, e := recovery.Rebuild(ctx); rebuilt <- outcome{p, i, e} }()
	select {
	case out := <-rebuilt:
		t.Fatalf("Worker 未追平就结束重建: %v", out.err)
	case <-time.After(700 * time.Millisecond):
	}
	startWorker()
	workerRunning = true
	out := <-rebuilt
	if out.err != nil {
		t.Fatal(out.err)
	}
	projection, initializer = out.projection, out.initializer
	t.Run("rebuild_waits_for_worker_and_preserves_zero_session", func(t *testing.T) {
		if projection.epoch == epoch {
			t.Fatal("没有创建新代次")
		}
		for session, want := range map[string]string{"1": "23", "2": "0"} {
			state, err := projection.ReadSession(ctx, session, session)
			if err != nil || state.Count != want {
				t.Fatalf("场次%s: %+v %v", session, state, err)
			}
		}
		board, err := projection.ReadBoard(ctx, "1", "1", "1", "")
		if err != nil || board.Personal != "16" || len(board.Top) != 2 || board.Top[0].Count != "16" {
			t.Fatalf("榜单恢复不正确: %+v %v", board, err)
		}
		loaded, _, err := recovery.Check(ctx)
		if err != nil || loaded.epoch != projection.epoch {
			t.Fatalf("重启没有读取有效代次: %v", err)
		}
	})
	rt, err := NewConsumer(ctx, []string{broker}, "live-realtime")
	if err != nil {
		t.Fatal(err)
	}
	rtCtx, stopRT := context.WithCancel(ctx)
	rtDone := make(chan error, 1)
	var handled atomic.Int32
	go func() {
		rtDone <- rt.Run(rtCtx, func(ctx context.Context, record ConsumedLike) error {
			if err := initializer.Ensure(ctx, record.Event); err != nil {
				return err
			}
			_, err := projection.ApplyLike(ctx, record)
			if err == nil {
				handled.Add(1)
			}
			return err
		})
	}()
	publish(first)
	extra, _ := NewLikeBatch("1", "1", "1", uuid.Must(uuid.NewV7()).String(), 3, time.Now())
	publish(extra)
	waitFor(func() bool {
		state, e := projection.ReadSession(ctx, "1", "1")
		var n int64
		db.Table("interaction_events").Count(&n)
		return e == nil && state.Count == "26" && n == 4 && handled.Load() >= 2
	})
	stopRT()
	if err := <-rtDone; err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	rt.Close()
	// 等待两组的最后一次事务/提交结束，后续只验证有缺口时的拒绝。
	waitFor(func() bool {
		end, e := worker.logOffsets(ctx, -1)
		if e != nil {
			return false
		}
		committed, e := worker.CommittedOffsets(ctx)
		if e != nil {
			return false
		}
		for p, n := range end {
			if max(committed[p], 0) != n {
				return false
			}
		}
		return true
	})
	finishWorker()
	workerRunning = false
	t.Run("equal_offsets_still_require_dedup", func(t *testing.T) {
		key := "ls:v1:projection:" + projection.epoch + ":processed:" + extra.EventID
		client.Del(ctx, key)
		if _, _, err := recovery.Check(ctx); !errors.Is(err, ErrProjectionRecovery) {
			t.Fatalf("相等位移下遗漏去重数据: %v", err)
		}
		client.Set(ctx, key, extra.UserID+":"+extra.RequestHash, 9*24*time.Hour)
	})
	t.Run("mysql_count_conflict_is_blocked", func(t *testing.T) {
		db.Exec("UPDATE interaction_events SET click_count=6 WHERE event_id=?", first.EventID)
		if _, _, err := recovery.Rebuild(ctx); !errors.Is(err, ErrRecoveryBlocked) {
			t.Fatalf("明细冲突没有阻止恢复: %v", err)
		}
		db.Exec("UPDATE interaction_events SET click_count=5 WHERE event_id=?", first.EventID)
	})
	t.Run("redis_behind_requires_rebuild", func(t *testing.T) {
		partition := records[0].Partition
		client.Set(ctx, "ls:v1:projection:"+projection.epoch+":partition:"+strconv.Itoa(int(partition)), 0, 0)
		if _, _, err := recovery.Check(ctx); !errors.Is(err, ErrProjectionRecovery) {
			t.Fatalf("Redis 回退被错误放行: %v", err)
		}
	})
	t.Run("missing_sql_history_is_blocked", func(t *testing.T) {
		db.Exec("DELETE FROM consumer_inbox WHERE event_id=?", first.EventID)
		if _, _, err := recovery.Rebuild(ctx); !errors.Is(err, ErrRecoveryBlocked) {
			t.Fatalf("去重明细缺失未阻止恢复: %v", err)
		}
	})
	t.Run("expired_unpersisted_log_is_blocked", func(t *testing.T) {
		end, err := admin.logOffsets(ctx, -1)
		if err != nil {
			t.Fatal(err)
		}
		partition := records[0].Partition
		req := kmsg.NewPtrDeleteRecordsRequest()
		req.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: "live.events", Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: partition, Offset: end[partition]}}}}
		if _, err := req.RequestWith(ctx, admin.client); err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("UPDATE worker_checkpoints SET next_offset=0 WHERE partition_id=?", partition).Error; err != nil {
			t.Fatal(err)
		}
		if _, _, err := recovery.Rebuild(ctx); !errors.Is(err, ErrRecoveryBlocked) || !strings.Contains(err.Error(), "日志起点") {
			t.Fatalf("日志缺口没有阻止恢复: %v", err)
		}
	})
	t.Logf("normal restart, ahead replay, missing marker, Worker backlog, rebuild, zero session, retry, conflict and missing-log checks completed; epoch=%s", projection.epoch)
}

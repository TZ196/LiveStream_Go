package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/messaging"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	probeGroup           = "live-realtime"
	probeRebalanceBudget = 60 * time.Second
)

func TestLikeMutexHandoff(t *testing.T) {
	enableLockProbe(t)
	checkLockProbeExit(t)
	broker, address := os.Getenv("LIVESTREAM_TEST_KAFKA"), os.Getenv("LIVESTREAM_TEST_REDIS")
	cluster := os.Getenv("LIVESTREAM_TEST_LOCK_CLUSTER_ID")
	if broker == "" || address == "" || cluster == "" {
		t.Skip("需要隔离测试 Kafka、Redis 及其 Kafka 集群 ID")
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	check, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	info, err := kmsg.NewPtrMetadataRequest().RequestWith(check, admin)
	if err != nil || info.ClusterID == nil || *info.ClusterID != cluster {
		t.Fatalf("无法核对隔离 Kafka 集群身份：%v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()

	for _, trial := range []struct {
		name        string
		cancellable bool
		hold        time.Duration
	}{
		{"Mutex", false, probeHold},
		{"ContextMutex", true, probeHold},
		{"MutexPastRebalanceTimeout", false, 65 * time.Second},
	} {
		t.Run(trial.name, func(t *testing.T) {
			runLikeLockHandoff(t, broker, admin, client, trial.cancellable, trial.hold)
		})
	}
}

type lockProbeEntry struct {
	record *kgo.Record
	work   context.Context
	start  time.Time
}

type lockProbeReturn struct {
	at  time.Time
	err error
}

// 使用真实消费循环、Redis Lua、房间快照队列。实验从已初始化的场次开始，单独观察实时组。
func runLikeLockHandoff(t *testing.T, broker string, admin *kgo.Client, redisClient *redis.Client, cancellable bool, holdFor time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), holdFor+probeRebalanceBudget)
	defer cancel()
	epoch := uuid.NewString()
	repo, err := messaging.NewProjectionRepository(redisClient, epoch)
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	event, err := messaging.NewLikeBatch("7", "1", "2", requestID.String(), 5, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.InitializeNewSession(ctx, event.RoomID, event.SessionID); err != nil {
		t.Fatal(err)
	}

	manager := NewManager(nil, nil, repo, nil, nil, epoch)
	manager.SetAvailable(true)
	viewer := &connection{send: make(chan []byte, 2)}
	manager.audiences[2] = &audience{roomID: 1, clients: map[*connection]struct{}{viewer: {}}}
	gate := newProbeContextMutex()
	brokers := []string{broker}
	writer, err := messaging.NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	first, err := messaging.NewConsumer(ctx, brokers, probeGroup)
	if err != nil {
		t.Fatal(err)
	}
	closeFirst := sync.OnceFunc(first.Close)
	var next *messaging.Consumer
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
		closeFirst()
		if next != nil {
			next.Close()
		}
	}()

	// 普通组锁住服务的真实 manager.mu。对照组在同一业务位置加实验锁，拿到后复用原队列逻辑。
	hold, locked := make(chan struct{}), make(chan struct{})
	holderDone := make(chan struct{})
	workers.Go(func() {
		defer close(holderDone)
		select {
		case <-hold:
		case <-ctx.Done():
			return
		}
		if cancellable {
			if gate.lock(ctx) != nil {
				return
			}
			defer gate.unlock()
		} else {
			manager.mu.Lock()
			defer manager.mu.Unlock()
		}
		close(locked)
		select {
		case <-time.After(holdFor):
		case <-ctx.Done():
		}
	})
	waiting := make(chan lockProbeEntry, 1)
	returned := make(chan lockProbeReturn, 1)
	firstDone := make(chan error, 1)
	workers.Go(func() {
		firstDone <- first.Run(ctx, func(work context.Context, record *kgo.Record, _ string) error {
			start := time.Now()
			in, err := messaging.DecodeLikeRecord(record)
			if err != nil {
				return err
			}
			if in.EventID != event.EventID {
				return errors.New("隔离组中存在未处理的其他记录")
			}
			result, err := repo.ApplyLike(work, record)
			if err != nil {
				return err
			}
			if result.Duplicate || result.Count != "5" {
				return fmt.Errorf("首次 Redis 投影异常：%+v", result)
			}
			close(hold)
			select {
			case <-locked:
			case <-work.Done():
				return work.Err()
			}
			waiting <- lockProbeEntry{record: record, work: work, start: start}
			if cancellable {
				err = gate.lock(work)
				if err == nil {
					defer gate.unlock()
				}
			}
			if err == nil {
				manager.QueueLikeUpdate(in, result)
			}
			returned <- lockProbeReturn{at: time.Now(), err: err}
			return err
		})
	})
	if err = writer.PublishLike(ctx, event); err != nil {
		t.Fatal(err)
	}
	var entry lockProbeEntry
	select {
	case entry = <-waiting:
	case err = <-firstDone:
		t.Fatalf("首成员未进入锁等待：%v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	record := entry.record
	before := probeOffset(t, ctx, first, record.Partition)
	if before >= record.Offset+1 {
		t.Fatalf("处理中已提交：%d", before)
	}
	checkProbeProjection(t, ctx, repo, redisClient, epoch, event, record)
	t.Logf("Redis 已写入 5 赞、指纹及检查点 %d，Kafka p=%d offset=%d，已提交位置 %d", record.Offset+1, record.Partition, record.Offset, before)
	original := readProbeGroup(t, ctx, admin)
	if original.State != "Stable" || len(original.Members) != 1 {
		t.Fatal("实验开始时应只有旧成员持有分区")
	}
	oldID := original.Members[0].MemberID

	join := time.Now()
	next, err = messaging.NewConsumer(ctx, brokers, probeGroup)
	if err != nil {
		t.Fatal(err)
	}
	type receipt struct {
		record *kgo.Record
		at     time.Time
	}
	reread, permit := make(chan receipt, 1), make(chan struct{})
	replayed := make(chan messaging.ProjectedLike, 1)
	nextDone := make(chan error, 1)
	workers.Go(func() {
		nextDone <- next.Run(ctx, func(work context.Context, row *kgo.Record, _ string) error {
			in, err := messaging.DecodeLikeRecord(row)
			if err != nil {
				return err
			}
			if in.EventID != event.EventID {
				return errors.New("接手者读到其他事件")
			}
			reread <- receipt{record: row, at: time.Now()}
			// 暂缓接手者提交，以便分辨旧成员是否错误提交过该条记录。
			select {
			case <-permit:
			case <-work.Done():
				return work.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
			result, err := repo.ApplyLike(work, row)
			if err != nil {
				return err
			}
			if !result.Duplicate {
				return errors.New("Redis 未识别重放")
			}
			manager.QueueLikeUpdate(in, result)
			replayed <- result
			return nil
		})
	})
	started := probeGroupState(t, entry.work, admin, "")
	t.Logf("B 等锁 %.3fs 时 Broker 进入 %s", time.Since(entry.start).Seconds(), started)
	<-entry.work.Done()
	if entry.work.Err() != context.DeadlineExceeded {
		t.Fatal(entry.work.Err())
	}
	if now := probeOffset(t, ctx, next, record.Partition); now != before {
		t.Fatalf("截止时位移错进：%d", now)
	}
	// 跨过 60 秒的额外场景允许接手者立即完成，避免观测本身拖慢组协调。
	pastBudget := holdFor > probeRebalanceBudget
	if pastBudget {
		close(permit)
		select {
		case <-time.After(time.Until(join.Add(probeRebalanceBudget))):
		case <-returned:
			t.Fatal("到达交接预算前普通锁已意外返回")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		group := readProbeGroup(t, ctx, admin)
		t.Logf("加入后 60s 旧处理仍在等锁，Broker 组状态 %s，已提交位置 %d", group.State, probeOffset(t, ctx, next, record.Partition))
		beforeUnlock, stop := context.WithDeadline(ctx, entry.start.Add(holdFor))
		defer stop()
		probeGroupState(t, beforeUnlock, admin, oldID)
		select {
		case <-returned:
			t.Fatal("未观察到旧处理仍在等锁时发生的所有权交接")
		default:
		}
		t.Logf("新成员在加入后 %.3fs 已独占 16 个分区，旧处理仍在等锁", time.Since(join).Seconds())
	}
	var result lockProbeReturn
	select {
	case result = <-returned:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waited := result.at.Sub(entry.start)
	if cancellable {
		if !errors.Is(result.err, context.DeadlineExceeded) || waited >= holdFor {
			t.Fatalf("可取消锁未及时返回：%v %v", waited, result.err)
		}
	} else if result.err != nil || waited < holdFor {
		t.Fatalf("普通锁未等到释放：%v %v", waited, result.err)
	}
	select {
	case err = <-firstDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("旧成员应报告本条记录超时：%v", err)
	}
	commitFailed := strings.Contains(err.Error(), "提交 Kafka 位移")
	if cancellable && commitFailed || !cancellable && !commitFailed {
		t.Fatalf("旧成员退出位置异常：%v", err)
	}
	if !pastBudget {
		if now := probeOffset(t, ctx, next, record.Partition); now != before {
			t.Fatalf("旧成员已误提交：%d", now)
		}
		t.Logf("允许接手者提交前，Broker 上的位移仍为 %d", before)
	}
	t.Logf("处理函数 %.3fs 返回 %v，消费循环返回 %v", waited.Seconds(), result.err, err)
	closeFirst()
	select {
	case read := <-reread:
		row := read.record
		if row.Partition != record.Partition || row.Offset != record.Offset {
			t.Fatalf("接手者位移不一致：%d/%d", row.Partition, row.Offset)
		}
		t.Logf("新成员在加入请求后 %.3fs 重读该记录", read.at.Sub(join).Seconds())
	case err := <-nextDone:
		t.Fatalf("接手者提前退出：%v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !pastBudget {
		close(permit)
	}
	select {
	case duplicate := <-replayed:
		if duplicate.Personal != "5" || duplicate.Count != "5" {
			t.Fatal(duplicate)
		}
	case err := <-nextDone:
		t.Fatalf("重放失败：%v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 从加入请求开始计时，比从 Broker 察觉成员变化起算更保守。
	end := join.Add(probeRebalanceBudget)
	if pastBudget {
		end = end.Add(holdFor)
	}
	budget, stop := context.WithDeadline(ctx, end)
	defer stop()
	probeGroupState(t, budget, admin, oldID)
	for probeOffset(t, budget, next, record.Partition) != record.Offset+1 {
		select {
		case <-budget.Done():
			t.Fatal(budget.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Logf("加入请求后 %.3fs 已确认组稳定且新成员拥有 16 个分区，重读 %d，提交 %d",
		time.Since(join).Seconds(), record.Offset, record.Offset+1)
	checkProbeProjection(t, ctx, repo, redisClient, epoch, event, record)
	manager.flushLikeUpdates()
	frames := len(viewer.send)
	if cancellable && frames != 0 || !cancellable && frames != 1 {
		t.Fatalf("内存总数快照数异常：%d", frames)
	}
	if frames == 1 {
		var frame struct{ Data likeUpdate }
		if err := json.Unmarshal(<-viewer.send, &frame); err != nil || frame.Data.Count != "5" {
			t.Fatal(frame, err)
		}
	}
	t.Logf("重放后 Redis 场次/个人/榜首均为 5，观众队列可发的总数快照 %d", frames)
	// 即使可取消的处理先完成，A 也要持满计划时长。
	select {
	case <-holderDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func probeOffset(t *testing.T, ctx context.Context, c *messaging.Consumer, partition int32) int64 {
	t.Helper()
	values, err := c.CommittedOffsets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pos, ok := values[partition]
	if !ok {
		t.Fatalf("分区 %d 无查询结果", partition)
	}
	return pos
}

// oldID 为空时观察重平衡开始，有值时等待原成员退出且新成员独占全部分区。
func probeGroupState(t *testing.T, ctx context.Context, admin *kgo.Client, oldID string) string {
	t.Helper()
	for {
		group := readProbeGroup(t, ctx, admin)
		if oldID == "" && (group.State == "PreparingRebalance" || group.State == "CompletingRebalance") {
			return group.State
		}
		if oldID != "" && group.State == "Stable" && len(group.Members) == 1 && group.Members[0].MemberID != oldID {
			var assignment kmsg.ConsumerMemberAssignment
			if err := assignment.ReadFrom(group.Members[0].MemberAssignment); err != nil {
				t.Fatal(err)
			}
			for _, topic := range assignment.Topics {
				if topic.Topic == "live.events" && len(topic.Partitions) == 16 {
					return group.State
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func readProbeGroup(t *testing.T, ctx context.Context, admin *kgo.Client) kmsg.DescribeGroupsResponseGroup {
	t.Helper()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{probeGroup}
	reply, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Groups) != 1 || reply.Groups[0].ErrorCode != 0 {
		t.Fatalf("无法读取消费组状态：%+v", reply.Groups)
	}
	return reply.Groups[0]
}

func checkProbeProjection(t *testing.T, ctx context.Context, repo *messaging.ProjectionRepository, client *redis.Client, epoch string, event messaging.LikeEvent, record *kgo.Record) {
	t.Helper()
	prefix := "ls:v1:projection:" + epoch + ":"
	seen, err := client.Get(ctx, prefix+"processed:"+event.EventID).Result()
	if err != nil || seen != event.UserID+":"+event.RequestHash {
		t.Fatal("Redis 事件指纹错误", err)
	}
	position, err := client.Get(ctx, prefix+"partition:"+strconv.Itoa(int(record.Partition))).Int64()
	if err != nil || position != record.Offset+1 {
		t.Fatal("Redis 分区检查点错误", position, err)
	}
	total, err := repo.ReadSession(ctx, event.RoomID, event.SessionID)
	if err != nil || total.Count != "5" || total.Revision != "5" {
		t.Fatal(total, err)
	}
	board, err := repo.ReadBoard(ctx, event.RoomID, event.SessionID, event.UserID, "")
	if err != nil || board.Personal != "5" || len(board.Top) != 1 || board.Top[0] != (messaging.RankedUser{UserID: event.UserID, Count: "5"}) {
		t.Fatal(board, err)
	}
}

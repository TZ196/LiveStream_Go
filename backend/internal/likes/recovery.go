package likes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var (
	ErrProjectionRecovery = errors.New("Redis 投影需要重建")
	ErrRecoveryBlocked    = errors.New("点赞恢复条件不足")
)

// Recovery 由单个 API 的消费协程串行调用，不创建恢复任务、租约或断点记录。
type Recovery struct {
	db       *gorm.DB
	client   *redis.Client
	brokers  []string
	fallback string
}

func NewRecovery(db *gorm.DB, client *redis.Client, brokers []string, fallback string) *Recovery {
	return &Recovery{db: db, client: client, brokers: brokers, fallback: fallback}
}

func redisRunID(ctx context.Context, client *redis.Client) (string, error) {
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "run_id:")), nil
		}
	}
	return "", errors.New("无法识别 Redis 运行编号")
}

// Check 保留正常投影；Redis 可以领先提交位置，但领先区间必须能证明已去重。
func (recovery *Recovery) Check(ctx context.Context) (*ProjectionRepository, *ProjectionInitializer, error) {
	admin, err := newKafkaClient(ctx, recovery.brokers, "live-realtime")
	if err != nil {
		return nil, nil, err
	}
	defer admin.Close()
	if err = admin.requireInactive(ctx); err != nil {
		return nil, nil, err
	}
	persistence := NewPersistenceRepository(recovery.db)
	epoch, err := persistence.ProjectionEpoch(ctx, recovery.fallback, admin.topicID)
	if err != nil {
		return nil, nil, err
	}
	projection, _ := NewProjectionRepository(recovery.client, epoch)
	projection.topicID = admin.topicID
	projection.runID, err = redisRunID(ctx, recovery.client)
	if err != nil {
		return nil, nil, err
	}
	initializer := NewProjectionInitializer(recovery.db, projection)
	var missing bool
	err = recovery.db.WithContext(ctx).Raw(`SELECT EXISTS (
        SELECT 1 FROM interaction_events e LEFT JOIN projection_sessions p ON p.session_id=e.session_id AND p.epoch=?
        WHERE p.session_id IS NULL
    )`, epoch).Scan(&missing).Error
	if err != nil {
		return nil, nil, err
	}
	if missing {
		return nil, nil, fmt.Errorf("%w: 已保存的点赞缺少场次投影", ErrProjectionRecovery)
	}
	committed, err := admin.CommittedOffsets(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err = initializer.ValidateProjectionStartup(ctx, admin.topicID, committed); err != nil {
		return nil, nil, err
	}
	starts, err := admin.logOffsets(ctx, -2)
	if err != nil {
		return nil, nil, err
	}
	ends, err := admin.logOffsets(ctx, -1)
	if err != nil {
		return nil, nil, err
	}
	projected := make(map[int32]int64, 16)
	for partition, offset := range committed {
		raw, readErr := recovery.client.Get(ctx, "ls:v1:projection:"+epoch+":partition:"+strconv.Itoa(int(partition))).Result()
		if errors.Is(readErr, redis.Nil) {
			raw = "0"
		} else if readErr != nil {
			return nil, nil, readErr
		}
		current, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || current < 0 || current > ends[partition] || offset > ends[partition] {
			return nil, nil, fmt.Errorf("%w: 分区 %d 的位置超出日志范围", ErrRecoveryBlocked, partition)
		}
		if max(offset, 0) < starts[partition] {
			return nil, nil, fmt.Errorf("%w: 分区 %d 的待重放日志已过期，需要从明细核对", ErrProjectionRecovery, partition)
		}
		projected[partition] = current
	}
	if err = admin.verifyReplay(ctx, recovery.brokers, projection, starts, committed, projected); err != nil {
		return nil, nil, err
	}
	currentRun, err := redisRunID(ctx, recovery.client)
	if err != nil {
		return nil, nil, err
	}
	if currentRun != projection.runID {
		return nil, nil, fmt.Errorf("%w: 检查期间 Redis 再次重启", ErrProjectionRecovery)
	}
	if err = recovery.client.Set(ctx, "ls:v1:projection:"+epoch+":identity", admin.topicID+":"+projection.runID, 0).Err(); err != nil {
		return nil, nil, err
	}
	return projection, initializer, nil
}

// Rebuild 的调用方必须先停止新准入、排空 Producer 并退出实时消费组。
// Worker 独立追平后自然待机；所有检查通过前，新代次始终不对客户端启用。
func (recovery *Recovery) Rebuild(ctx context.Context) (*ProjectionRepository, *ProjectionInitializer, error) {
	realtime, err := newKafkaClient(ctx, recovery.brokers, "live-realtime")
	if err != nil {
		return nil, nil, err
	}
	defer realtime.Close()
	if err = realtime.requireInactive(ctx); err != nil {
		return nil, nil, err
	}
	worker, err := newKafkaClient(ctx, recovery.brokers, "live-persistence")
	if err != nil {
		return nil, nil, err
	}
	defer worker.Close()
	if worker.topicID != realtime.topicID {
		return nil, nil, fmt.Errorf("%w: Topic 身份不一致", ErrRecoveryBlocked)
	}
	persistence := NewPersistenceRepository(recovery.db)
	previous, err := persistence.ProjectionEpoch(ctx, recovery.fallback, realtime.topicID)
	if err != nil {
		return nil, nil, err
	}
	barrier, err := realtime.logOffsets(ctx, -1)
	if err != nil {
		return nil, nil, err
	}
	if err = recovery.waitForWorker(ctx, worker, barrier); err != nil {
		return nil, nil, err
	}
	projection, _ := NewProjectionRepository(recovery.client, uuid.NewString())
	projection.topicID = realtime.topicID
	projection.runID, err = redisRunID(ctx, recovery.client)
	if err != nil {
		return nil, nil, err
	}
	complete := false
	activationStarted := false
	defer func() {
		if !complete && !activationStarted {
			// 只清理本次创建且未启用的随机前缀，原投影始终保留。
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var cursor uint64
			for {
				keys, next, scanErr := recovery.client.Scan(cleanup, cursor, "ls:v1:projection:"+projection.epoch+":*", 256).Result()
				if scanErr != nil {
					break
				}
				if len(keys) > 0 {
					if recovery.client.Del(cleanup, keys...).Err() != nil {
						break
					}
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
			_ = recovery.db.WithContext(cleanup).Where("epoch = ?", projection.epoch).Delete(&projectionSession{}).Error
		}
	}()
	initializer := NewProjectionInitializer(recovery.db, projection)
	err = recovery.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		snapshot := NewPersistenceRepository(tx)
		if err := snapshot.validateCheckpoints(ctx, realtime.topicID, barrier, true); err != nil {
			return fmt.Errorf("%w: %v", ErrRecoveryBlocked, err)
		}
		if err := snapshot.validateRecoveryHistory(ctx); err != nil {
			return err
		}
		var after uint64
		for {
			sessions, err := snapshot.recoverySessions(ctx, after)
			if err != nil {
				return err
			}
			if len(sessions) == 0 {
				break
			}
			for _, session := range sessions {
				if err := recovery.buildSession(ctx, snapshot, projection, session); err != nil {
					return err
				}
				session.Epoch, session.Status = projection.epoch, "READY"
				if err := tx.Create(&session).Error; err != nil {
					return err
				}
				after = session.SessionID
			}
		}
		afterEvent := ""
		now := time.Now()
		for {
			events, err := snapshot.recoveryEvents(ctx, afterEvent)
			if err != nil {
				return err
			}
			if len(events) == 0 {
				break
			}
			pipe := recovery.client.Pipeline()
			for _, event := range events {
				// 只恢复仍处于保护窗口的标记，重复读取不会无限延长窗口。
				ttl := 9*24*time.Hour - now.Sub(event.EventTime)
				if ttl > 9*24*time.Hour {
					ttl = 9 * 24 * time.Hour
				}
				if ttl > 0 {
					pipe.Set(ctx, "ls:v1:projection:"+projection.epoch+":processed:"+event.EventID, strconv.FormatUint(event.UserID, 10)+":"+event.RequestHash, ttl)
				}
				afterEvent = event.EventID
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, nil, err
	}
	pipe := recovery.client.Pipeline()
	for partition, offset := range barrier {
		pipe.Set(ctx, "ls:v1:projection:"+projection.epoch+":partition:"+strconv.Itoa(int(partition)), offset, 0)
	}
	pipe.Set(ctx, "ls:v1:projection:"+projection.epoch+":identity", realtime.topicID+":"+projection.runID, 0)
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, nil, err
	}
	if err = initializer.ValidateProjectionStartup(ctx, realtime.topicID, barrier); err != nil {
		return nil, nil, err
	}
	end, err := realtime.logOffsets(ctx, -1)
	if err != nil {
		return nil, nil, err
	}
	if !maps.Equal(end, barrier) {
		return nil, nil, fmt.Errorf("%w: 暂停期间仍有生产者写入", ErrRecoveryBlocked)
	}
	runID, err := redisRunID(ctx, recovery.client)
	if err != nil {
		return nil, nil, err
	}
	if runID != projection.runID {
		return nil, nil, errors.New("Redis 运行编号在重建期间改变")
	}
	// 切换开始后保留已验证的新数据，避免结果未知的数据库更新已成功而被清理。
	activationStarted = true
	if err = realtime.resetOffsets(ctx, barrier); err != nil {
		return nil, nil, err
	}
	if err = persistence.saveProjectionEpoch(ctx, previous, projection.epoch, realtime.topicID); err != nil {
		return nil, nil, err
	}
	complete = true
	return projection, initializer, nil
}

func (recovery *Recovery) waitForWorker(ctx context.Context, worker *Consumer, barrier map[int32]int64) error {
	persistence := NewPersistenceRepository(recovery.db)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		starts, err := worker.logOffsets(ctx, -2)
		if err != nil {
			return err
		}
		ends, err := worker.logOffsets(ctx, -1)
		if err != nil {
			return err
		}
		if !maps.Equal(ends, barrier) {
			return fmt.Errorf("%w: 暂停期间仍有生产者写入", ErrRecoveryBlocked)
		}
		positions, err := persistence.checkpoints(ctx, worker.topicID)
		if err != nil {
			return err
		}
		committed, err := worker.CommittedOffsets(ctx)
		if err != nil {
			return err
		}
		ready := true
		for partition, end := range barrier {
			position, commit := positions[partition], max(committed[partition], 0)
			if position < starts[partition] || commit < starts[partition] {
				return fmt.Errorf("%w: 分区 %d 的 Worker 恢复位置早于日志起点", ErrRecoveryBlocked, partition)
			}
			if position > end || commit > position {
				return fmt.Errorf("%w: 分区 %d 的数据库检查点不能证明已落库范围", ErrRecoveryBlocked, partition)
			}
			if position != end || commit != end {
				ready = false
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("等待 Worker 追平超时或已取消: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (recovery *Recovery) buildSession(ctx context.Context, snapshot *PersistenceRepository, projection *ProjectionRepository, session projectionSession) error {
	key := projection.sessionKey(strconv.FormatUint(session.SessionID, 10))
	var after uint64
	var total, participants int64
	for {
		counts, err := snapshot.recoveryCounts(ctx, session.SessionID, after)
		if err != nil {
			return err
		}
		if len(counts) == 0 {
			break
		}
		args := make([]any, 0, len(counts)*2)
		for _, count := range counts {
			if count.Clicks < 1 || total > math.MaxInt64-count.Clicks {
				return fmt.Errorf("%w: 场次点赞汇总无效或溢出", ErrRecoveryBlocked)
			}
			total += count.Clicks
			participants++
			args = append(args, strconv.FormatUint(count.UserID, 10), strconv.FormatInt(count.Clicks, 10))
			after = count.UserID
		}
		if err := stageBoard.Run(ctx, recovery.client, []string{key + ":users", key + ":top"}, args...).Err(); err != nil {
			return err
		}
	}
	pipe := recovery.client.Pipeline()
	pipe.HSet(ctx, key, "room_id", session.RoomID, "likes", total, "revision", "0", "initialized", "1", "board_users", participants, "board_revision", "0", "board_through", "0")
	pipe.Persist(ctx, key+":users")
	pipe.Persist(ctx, key+":top")
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	initializer := NewProjectionInitializer(recovery.db, projection)
	if err := initializer.validateSession(ctx, session.RoomID, session.SessionID); err != nil {
		return fmt.Errorf("重建场次 %d 校验失败: %w", session.SessionID, err)
	}
	return nil
}

// CheckInstance 由运行期监测调用；计数脚本也在原子写入前检查同一编号。
func (repository *ProjectionRepository) CheckInstance(ctx context.Context) error {
	current, err := redisRunID(ctx, repository.client)
	if err != nil {
		return err
	}
	if repository.runID != "" && current != repository.runID {
		return fmt.Errorf("%w: Redis 已重启", ErrProjectionRecovery)
	}
	identity, err := repository.client.Get(ctx, "ls:v1:projection:"+repository.epoch+":identity").Result()
	if errors.Is(err, redis.Nil) {
		return fmt.Errorf("%w: Redis 投影身份丢失", ErrProjectionRecovery)
	}
	if err != nil {
		return err
	}
	if repository.topicID != "" && identity != repository.topicID+":"+repository.runID {
		return fmt.Errorf("%w: Redis 投影身份发生变化", ErrProjectionRecovery)
	}
	return nil
}

func (repository *ProjectionRepository) Epoch() string { return repository.epoch }

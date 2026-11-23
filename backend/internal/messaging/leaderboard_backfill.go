package messaging

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// 维护窗口内先核对两个组均已退出，而且每一分区已处理至日志末端。
func drainedOffsets(ctx context.Context, c *Consumer) (map[int32]int64, error) {
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{c.group}
	group, err := describe.RequestWith(ctx, c.client)
	if err != nil {
		return nil, err
	}
	if len(group.Groups) != 1 {
		return nil, errors.New("无法核对消费组成员")
	}
	info := group.Groups[0]
	if err := kerr.ErrorForCode(info.ErrorCode); err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, err
	}
	if len(info.Members) > 0 {
		return nil, fmt.Errorf("请先停止 %s 的所有成员", c.group)
	}
	committed, err := c.CommittedOffsets(ctx)
	if err != nil {
		return nil, err
	}
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.ListOffsetsRequestTopic{Topic: "live.events"}
	for _, id := range c.partitions {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = id
		part.Timestamp = -1
		topic.Partitions = append(topic.Partitions, part)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	end, err := request.RequestWith(ctx, c.client)
	if err != nil {
		return nil, err
	}
	if len(end.Topics) != 1 || len(end.Topics[0].Partitions) != len(committed) {
		return nil, errors.New("日志末端不完整")
	}
	for _, part := range end.Topics[0].Partitions {
		if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
			return nil, err
		}
		pos, exists := committed[part.Partition]
		if !exists || (pos != part.Offset && !(pos == -1 && part.Offset == 0)) {
			return nil, fmt.Errorf("%s 分区 %d 尚未追平，提交 %d，末端 %d", c.group, part.Partition, pos, part.Offset)
		}
	}
	return committed, nil
}

// BackfillLeaderboards 只补充排行榜，不改变既有场次计数、去重键或 Kafka 位移。
func (initializer *ProjectionInitializer) BackfillLeaderboards(ctx context.Context, brokers []string) error {
	rt, err := newKafkaClient(ctx, brokers, "live-realtime")
	if err != nil {
		return err
	}
	defer rt.Close()
	worker, err := newKafkaClient(ctx, brokers, "live-persistence")
	if err != nil {
		return err
	}
	defer worker.Close()
	if rt.TopicID() != worker.TopicID() {
		return errors.New("消费组 Topic 身份不一致")
	}
	checkBarrier := func() (map[int32]int64, error) {
		offsets, err := drainedOffsets(ctx, rt)
		if err != nil {
			return nil, err
		}
		persisted, err := drainedOffsets(ctx, worker)
		if err != nil {
			return nil, err
		}
		for part, pos := range offsets {
			if max(pos, 0) != max(persisted[part], 0) {
				return nil, errors.New("两个消费组不在同一屏障")
			}
		}
		if err := initializer.validateProjectionStartup(ctx, rt.TopicID(), offsets, true); err != nil {
			return nil, err
		}
		persistence := NewPersistenceRepository(initializer.db)
		if err := persistence.validateCheckpoints(ctx, worker.TopicID(), persisted, true); err != nil {
			return nil, err
		}
		return offsets, nil
	}
	before, err := checkBarrier()
	if err != nil {
		return err
	}
	var missing int64
	err = initializer.db.WithContext(ctx).Table("interaction_events AS e").
		Joins("LEFT JOIN projection_sessions AS p ON p.epoch = ? AND p.session_id = e.session_id", initializer.projection.epoch).
		Where("p.session_id IS NULL").Limit(1).Select("1").Scan(&missing).Error
	if err != nil {
		return err
	}
	if missing != 0 {
		return errors.New("已有点赞缺少场次投影记录，需要恢复")
	}
	var sessions []projectionSession
	if err = initializer.db.WithContext(ctx).Where("epoch = ?", initializer.projection.epoch).Find(&sessions).Error; err != nil {
		return err
	}
	for _, s := range sessions {
		if err = initializer.buildSessionBoard(ctx, s); err != nil {
			return fmt.Errorf("补建场次 %d: %w", s.SessionID, err)
		}
	}
	after, err := checkBarrier()
	if err != nil {
		return err
	}
	if !maps.Equal(before, after) {
		return errors.New("补建期间 Kafka 位移改变，请保持停写并重新补建")
	}
	return nil
}

// 临时键按页写入并续期。失败或中断时旧榜始终可辨识，暂存键会到期。
var stageBoard = redis.NewScript(leaderboardLua + `
for i = 1, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1])
  redis.call('ZADD', KEYS[2], 0, rank_member(ARGV[i+1], ARGV[i]))
end
redis.call('ZREMRANGEBYRANK', KEYS[2], 100, -1)
redis.call('EXPIRE', KEYS[1], 3600)
redis.call('EXPIRE', KEYS[2], 3600)
return 1
`)

var installBoard = redis.NewScript(`
local meta, users, top, staged_users, staged_top = unpack(KEYS)
local room, total, revision, participants = unpack(ARGV)
if redis.call('HGET', meta,'room_id') ~= room or redis.call('HGET',meta,'initialized') ~= '1'
   or redis.call('HGET',meta,'likes') ~= total or redis.call('HGET',meta,'revision') ~= revision then return redis.error_reply('BASELINE_MISMATCH') end
local n = tonumber(participants)
if redis.call('HLEN',staged_users) ~= n or redis.call('ZCARD',staged_top) ~= math.min(n,100) then return redis.error_reply('INCOMPLETE_BOARD') end
if n > 0 then
  redis.call('RENAME',staged_users,users)
  redis.call('RENAME',staged_top,top)
  redis.call('PERSIST',users)
  redis.call('PERSIST',top)
else
  redis.call('DEL',users,top)
end
redis.call('HSET',meta,'board_users',participants,'board_revision',revision,'board_through',revision)
return 1
`)

func (initializer *ProjectionInitializer) buildSessionBoard(ctx context.Context, session projectionSession) error {
	roomID := strconv.FormatUint(session.RoomID, 10)
	sessionID := strconv.FormatUint(session.SessionID, 10)
	repo := initializer.projection
	original, err := repo.ReadSession(ctx, roomID, sessionID)
	if err != nil {
		return err
	}
	rows, err := initializer.db.WithContext(ctx).Table("interaction_events").
		Select("user_id, SUM(click_count)").Where("session_id = ?", session.SessionID).Group("user_id").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	key := repo.sessionKey(sessionID)
	temp := key + ":staging:" + uuid.NewString()
	staged := []string{temp + ":users", temp + ":top"}
	var total, participants int64
	args := make([]any, 0, 512)
	for rows.Next() {
		var id uint64
		var count int64
		if err = rows.Scan(&id, &count); err != nil {
			return err
		}
		if count < 1 || total > math.MaxInt64-count {
			return errors.New("场次点赞汇总无效")
		}
		total += count
		participants++
		args = append(args, strconv.FormatUint(id, 10), strconv.FormatInt(count, 10))
		if len(args) == cap(args) {
			if err = stageBoard.Run(ctx, repo.client, staged, args...).Err(); err != nil {
				return err
			}
			args = args[:0]
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(args) > 0 {
		if err = stageBoard.Run(ctx, repo.client, staged, args...).Err(); err != nil {
			return err
		}
	}
	if strconv.FormatInt(total, 10) != original.Count {
		return fmt.Errorf("MySQL 点击数 %d 与 Redis %s 不一致", total, original.Count)
	}
	return installBoard.Run(ctx, repo.client, []string{key, key + ":users", key + ":top", staged[0], staged[1]}, roomID, total, original.Revision, participants).Err()
}

package likes

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Consumer 在单个消费组中按每次一条记录顺序处理和提交。
type Consumer struct {
	client     *kgo.Client
	topicID    string
	group      string
	partitions []int32
}

// NewConsumer 创建一个消费组成员，并要求 live.events 已存在且有稳定 Topic UUID。
func NewConsumer(ctx context.Context, brokers []string, group string) (*Consumer, error) {
	return newKafkaClient(ctx, brokers, group, kgo.ConsumerGroup(group), kgo.ConsumeTopics("live.events"),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()))
}

// 维护命令也要验证相同的 Topic 和分区；省略订阅选项时只查询元数据和位移，不参与组内分配。
func newKafkaClient(ctx context.Context, brokers []string, group string, opts ...kgo.Opt) (*Consumer, error) {
	if len(brokers) == 0 || (group != "live-realtime" && group != "live-persistence") {
		return nil, errors.New("Kafka 地址或消费组无效")
	}
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		return nil, err
	}
	request := kmsg.NewPtrMetadataRequest()
	name := "live.events"
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("获取 Kafka Topic 元数据: %w", err)
	}
	if len(response.Topics) != 1 || response.Topics[0].ErrorCode != 0 || response.Topics[0].TopicID == [16]byte{} {
		client.Close()
		return nil, errors.New("Kafka Topic 不存在或缺少稳定 UUID")
	}
	topicID := uuid.UUID(response.Topics[0].TopicID).String()
	if len(response.Topics[0].Partitions) != 16 {
		client.Close()
		return nil, errors.New("live.events 分区数必须为 16，变更需要迁移")
	}
	partitions := make([]int32, 0, 16)
	for _, part := range response.Topics[0].Partitions {
		if part.ErrorCode != 0 {
			client.Close()
			return nil, errors.New("Kafka 分区元数据不可用")
		}
		partitions = append(partitions, part.Partition)
	}
	return &Consumer{client: client, topicID: topicID, group: group, partitions: partitions}, nil
}

// TopicID 返回消费者绑定的 Kafka Topic UUID。
func (consumer *Consumer) TopicID() string { return consumer.topicID }

// Run 在处理成功后同步提交位移；失败时停止，避免越过未处理的记录。
// 调用方必须核对已存在投影与提交位移，并协调进程重启和恢复。
func (consumer *Consumer) Run(ctx context.Context, process func(context.Context, ConsumedLike) error) error {
	if process == nil {
		return errors.New("缺少消费处理函数")
	}
	for {
		fetches := consumer.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			consumer.client.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			consumer.client.AllowRebalance()
			return fmt.Errorf("Kafka 拉取失败: %v", errs)
		}
		var processingErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if processingErr != nil {
				return
			}
			// 停止拉取后，已取出的记录仍有独立期限完成业务写入和 offset 提交。
			recordCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer done()
			// 每组每条记录在入口校验一次，仓储和内存分发都使用同一份事件。
			decoded, err := DecodeLikeRecord(record, consumer.topicID)
			if err != nil {
				processingErr = fmt.Errorf("%w: Kafka 事件无效: %v", ErrRecoveryBlocked, err)
				return
			}
			if err := process(recordCtx, decoded); err != nil {
				processingErr = err
				return
			}
			if err := consumer.client.CommitRecords(recordCtx, record); err != nil {
				processingErr = fmt.Errorf("提交 Kafka 位移: %w", err)
			}
		})
		consumer.client.AllowRebalance()
		if processingErr != nil {
			return processingErr
		}
	}
}

// Close 释放消费组连接，调用前应取消 Run 的上下文。
func (consumer *Consumer) Close() { consumer.client.Close() }

// CommittedOffsets 读取消费组的已提交位置；-1 表示该分区尚无位置。
func (consumer *Consumer) CommittedOffsets(ctx context.Context) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = consumer.group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "live.events", Partitions: consumer.partitions}}
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = consumer.group
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: "live.events", Partitions: consumer.partitions}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{group}
	response, err := req.RequestWith(ctx, consumer.client)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]int64, len(consumer.partitions))
	if response.Version >= 8 {
		if len(response.Groups) != 1 {
			return nil, errors.New("读取 Kafka 消费组位移失败")
		}
		if err := kerr.ErrorForCode(response.Groups[0].ErrorCode); err != nil {
			return nil, fmt.Errorf("读取 Kafka 消费组位移: %w", err)
		}
		for _, topic := range response.Groups[0].Topics {
			if topic.Topic == "live.events" {
				for _, part := range topic.Partitions {
					if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
						return nil, fmt.Errorf("读取 Kafka 分区位移: %w", err)
					}
					result[part.Partition] = part.Offset
				}
			}
		}
	} else {
		if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
			return nil, fmt.Errorf("读取 Kafka 消费组位移: %w", err)
		}
		for _, topic := range response.Topics {
			if topic.Topic == "live.events" {
				for _, part := range topic.Partitions {
					if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
						return nil, fmt.Errorf("读取 Kafka 分区位移: %w", err)
					}
					result[part.Partition] = part.Offset
				}
			}
		}
	}
	if len(result) != len(consumer.partitions) {
		return nil, errors.New("Kafka 返回的分区位移不完整")
	}
	return result, nil
}

// logOffsets 查询日志起点（-2）或末端（-1）；位置都是下一条记录的位置。
func (consumer *Consumer) logOffsets(ctx context.Context, timestamp int64) (map[int32]int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.ListOffsetsRequestTopic{Topic: "live.events"}
	for _, partition := range consumer.partitions {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition, part.Timestamp = partition, timestamp
		topic.Partitions = append(topic.Partitions, part)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	response, err := request.RequestWith(ctx, consumer.client)
	if err != nil {
		return nil, err
	}
	if len(response.Topics) != 1 || response.Topics[0].Topic != "live.events" || len(response.Topics[0].Partitions) != 16 {
		return nil, errors.New("Kafka 日志位置不完整")
	}
	offsets := make(map[int32]int64, 16)
	for _, part := range response.Topics[0].Partitions {
		if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
			return nil, err
		}
		offsets[part.Partition] = part.Offset
	}
	return offsets, nil
}

func (consumer *Consumer) requireInactive(ctx context.Context) error {
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{consumer.group}
	response, err := request.RequestWith(ctx, consumer.client)
	if err != nil {
		return err
	}
	if len(response.Groups) != 1 {
		return errors.New("Kafka 消费组状态不完整")
	}
	group := response.Groups[0]
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return err
	}
	if len(group.Members) != 0 {
		return fmt.Errorf("消费组 %s 仍有活跃成员", consumer.group)
	}
	return nil
}

// resetOffsets 只在已停写且实时组全部退出的重建窗口使用，正常重启不调用。
func (consumer *Consumer) resetOffsets(ctx context.Context, offsets map[int32]int64) error {
	for {
		err := consumer.resetOffsetsOnce(ctx, offsets)
		if err == nil || !kerr.IsRetriable(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (consumer *Consumer) resetOffsetsOnce(ctx context.Context, offsets map[int32]int64) error {
	if err := consumer.requireInactive(ctx); err != nil {
		return err
	}
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = consumer.group
	topic := kmsg.OffsetCommitRequestTopic{Topic: "live.events"}
	for _, partition := range consumer.partitions {
		part := kmsg.NewOffsetCommitRequestTopicPartition()
		part.Partition = partition
		var exists bool
		part.Offset, exists = offsets[partition]
		if !exists || part.Offset < 0 {
			return errors.New("恢复位置不完整或无效")
		}
		topic.Partitions = append(topic.Partitions, part)
	}
	request.Topics = []kmsg.OffsetCommitRequestTopic{topic}
	response, err := request.RequestWith(ctx, consumer.client)
	if err != nil {
		return err
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 16 {
		return errors.New("恢复位置响应不完整")
	}
	for _, part := range response.Topics[0].Partitions {
		if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
			return err
		}
	}
	actual, err := consumer.CommittedOffsets(ctx)
	if err != nil {
		return err
	}
	if !maps.Equal(actual, offsets) {
		return errors.New("恢复位置读回校验失败")
	}
	return nil
}

// verifyReplay 核对仍保留日志内已经投影的事件。即使位置相等，也不能忽略去重键单独丢失。
// 此读取不加入消费组或提交位移；只有未提交的区间才受在线重放年龄限制。
func (consumer *Consumer) verifyReplay(ctx context.Context, brokers []string, projection *ProjectionRepository, starts, committed, projected map[int32]int64) error {
	partitions := make(map[int32]kgo.Offset)
	positions := make(map[int32]int64)
	for partition, end := range projected {
		start := starts[partition]
		if end > start {
			partitions[partition] = kgo.NewOffset().At(start)
			positions[partition] = start
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"live.events": partitions}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchMaxBytes(1024*1024), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		return err
	}
	defer reader.Close()
	for len(positions) > 0 {
		fetched := reader.PollRecords(ctx, 256)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if len(fetched.Errors()) != 0 {
			return fmt.Errorf("%w: 无法读取领先区间的去重依据", ErrProjectionRecovery)
		}
		for _, raw := range fetched.Records() {
			position, needed := positions[raw.Partition]
			if !needed {
				continue
			}
			if raw.Offset != position {
				return fmt.Errorf("%w: 分区 %d 的核验区间有缺口", ErrProjectionRecovery, raw.Partition)
			}
			record, err := DecodeLikeRecord(raw, consumer.topicID)
			if err != nil {
				return fmt.Errorf("%w: 核验日志事件无效: %v", ErrRecoveryBlocked, err)
			}
			age := time.Since(time.UnixMilli(record.Event.Timestamp))
			if raw.Offset >= max(committed[raw.Partition], 0) && age >= 8*24*time.Hour {
				return fmt.Errorf("%w: 重放超过在线去重窗口", ErrProjectionRecovery)
			}
			key := "ls:v1:projection:" + projection.epoch + ":processed:" + record.Event.EventID
			fingerprint, err := projection.client.Get(ctx, key).Result()
			// 保护窗口之外的已提交历史不再允许客户端重新发布，标记可正常到期。
			if age >= 9*24*time.Hour && errors.Is(err, redis.Nil) {
				err = nil
				fingerprint = record.Event.UserID + ":" + record.Event.RequestHash
			}
			if errors.Is(err, redis.Nil) {
				return fmt.Errorf("%w: 分区 %d 位于 %s 的事件缺少去重依据", ErrProjectionRecovery, raw.Partition, strconv.FormatInt(position, 10))
			}
			if err != nil {
				return err
			}
			if fingerprint != record.Event.UserID+":"+record.Event.RequestHash {
				return fmt.Errorf("%w: 领先区间事件指纹冲突", ErrRecoveryBlocked)
			}
			positions[raw.Partition] = record.NextOffset
			if record.NextOffset == projected[raw.Partition] {
				delete(positions, raw.Partition)
			}
		}
	}
	return nil
}

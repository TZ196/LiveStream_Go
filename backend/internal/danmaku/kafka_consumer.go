package danmaku

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
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
	metrics    *Metrics
}

// NewConsumer 创建一个消费组成员，并要求 live.danmaku 已存在且有稳定 Topic UUID。
func NewConsumer(ctx context.Context, brokers []string, group string) (*Consumer, error) {
	if group == RecentCacheGroup {
		return newKafkaClient(ctx, brokers, group, kgo.ConsumerGroup(group), kgo.ConsumeTopics(Topic),
			kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-5*time.Minute).UnixMilli())))
	}
	return newKafkaClient(ctx, brokers, group, kgo.ConsumerGroup(group), kgo.ConsumeTopics(Topic),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()))
}

// 维护命令也要验证相同的 Topic 和分区；省略订阅选项时只查询元数据和位移，不参与组内分配。
func newKafkaClient(ctx context.Context, brokers []string, group string, opts ...kgo.Opt) (*Consumer, error) {
	if len(brokers) == 0 || (group != RealtimeGroup && group != PersistenceGroup && group != RecentCacheGroup) {
		return nil, errors.New("Kafka 地址或消费组无效")
	}
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		return nil, err
	}
	request := kmsg.NewPtrMetadataRequest()
	name := Topic
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
		return nil, errors.New("live.danmaku 分区数必须为 16，变更需要迁移")
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

// Run 在业务处理或可靠隔离成功后提交位移；依赖故障不跳过记录。
// process 处理有效事件，quarantine 只在永久错误重试耗尽时保存原始记录。
func (consumer *Consumer) Run(ctx context.Context, process func(context.Context, Record) error, quarantine func(context.Context, FailedRecord) error) error {
	if process == nil || quarantine == nil {
		return errors.New("缺少消费处理或异常隔离函数")
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
			if err := consumer.processRecord(recordCtx, record, process, quarantine); err != nil {
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

// processRecord 的三次尝试共用单条记录预算；同一分区不会越过当前未完成记录。
func (consumer *Consumer) processRecord(ctx context.Context, raw *kgo.Record, process func(context.Context, Record) error, quarantine func(context.Context, FailedRecord) error) error {
	for attempt := 1; attempt <= 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := DecodeRecord(raw, consumer.topicID)
		code := "INVALID_EVENT"
		if err == nil {
			err = process(ctx, record)
			code = ""
			if errors.Is(err, ErrConflict) {
				code = "EVENT_CONFLICT"
			} else if errors.Is(err, ErrInvalidEvent) {
				code = "INVALID_EVENT"
			}
		}
		if err == nil {
			return nil
		}
		// 日志缺口、Topic 换代和恢复依据冲突不是单条坏消息，不能靠隔离解除保护。
		if errors.Is(err, ErrBlocked) {
			return err
		}
		if attempt < 3 {
			consumer.observeFailure("retry")
			timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		// 三次失败后，格式错误或内容冲突才交给该组的隔离回调。依赖故障保留原位移，等恢复后重读。
		if code == "" {
			consumer.observeFailure("dependency_failed")
			return fmt.Errorf("弹幕事件处理重试耗尽，保留原位移: %w", err)
		}
		failed := FailedRecord{Raw: raw, TopicID: consumer.topicID, Group: consumer.group, Code: code, Cause: err, Attempts: attempt}
		if err := quarantine(ctx, failed); err != nil {
			consumer.observeFailure("quarantine_failed")
			return fmt.Errorf("保存弹幕异常记录失败，保留原位移: %w", err)
		}
		consumer.observeFailure("quarantined")
		// 不将原始正文或任意解码错误写入运行日志，原文和有界摘要保存在异常表。
		slog.Warn("danmaku_record_quarantined", "group", consumer.group, "topic_id", consumer.topicID, "partition", raw.Partition, "offset", raw.Offset, "error_code", code, "attempts", attempt)
		return nil
	}
	return errors.New("弹幕记录未完成处理")
}

func (consumer *Consumer) observeFailure(result string) {
	if consumer.metrics != nil {
		consumer.metrics.Failures.WithLabelValues(consumer.group, result).Inc()
	}
}

// Close 释放消费组连接，调用前应取消 Run 的上下文。
func (consumer *Consumer) Close() { consumer.client.Close() }

// CommittedOffsets 读取消费组的已提交位置；-1 表示该分区尚无位置。
func (consumer *Consumer) CommittedOffsets(ctx context.Context) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = consumer.group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: Topic, Partitions: consumer.partitions}}
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = consumer.group
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: Topic, Partitions: consumer.partitions}}
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
			if topic.Topic == Topic {
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
			if topic.Topic == Topic {
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
	topic := kmsg.ListOffsetsRequestTopic{Topic: Topic}
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
	if len(response.Topics) != 1 || response.Topics[0].Topic != Topic || len(response.Topics[0].Partitions) != 16 {
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

// ConsumerGroup 在同一进程内持有多个独立 Kafka 成员，各成员只处理分配给自己的分区。
type ConsumerGroup struct {
	members []*Consumer
}

// OpenConsumerGroup 在所有成员开始拉取前完成一次全组恢复检查；失败时关闭已创建的成员。
func OpenConsumerGroup(ctx context.Context, brokers []string, name string, size int, validate func(context.Context, string, map[int32]int64) error) (*ConsumerGroup, error) {
	if size < 1 || size > 16 {
		return nil, errors.New("消费组成员数须在 1 到 16 之间")
	}
	group := &ConsumerGroup{}
	for i := 0; i < size; i++ {
		member, err := NewConsumer(ctx, brokers, name)
		if err != nil {
			group.Close()
			return nil, err
		}
		group.members = append(group.members, member)
		if member.TopicID() != group.members[0].TopicID() {
			group.Close()
			return nil, errors.New("创建消费组期间 Topic 身份改变")
		}
	}
	first := group.members[0]
	for {
		offsets, err := first.CommittedOffsets(ctx)
		if err == nil {
			starts, rangeErr := first.logOffsets(ctx, -2)
			if rangeErr == nil {
				var ends map[int32]int64
				ends, rangeErr = first.logOffsets(ctx, -1)
				if rangeErr == nil {
					for partition, offset := range offsets {
						if !(name == RecentCacheGroup && offset < 0) && (max(offset, 0) < starts[partition] || offset > ends[partition]) {
							rangeErr = fmt.Errorf("%w: %s 分区 %d 的消费位置超出保留日志范围", ErrBlocked, name, partition)
							break
						}
					}
				}
			}
			if rangeErr != nil {
				group.Close()
				return nil, rangeErr
			}
			ends, endErr := first.logOffsets(ctx, -1)
			if endErr != nil {
				group.Close()
				return nil, endErr
			}
			for partition, offset := range offsets {
				if offset < 0 && ends[partition] == 0 {
					offsets[partition] = 0
				}
			}
			err = validate(ctx, first.TopicID(), offsets)
			if err == nil {
				return group, nil
			}
			group.Close()
			return nil, fmt.Errorf("消费组 %s 恢复检查: %w", name, err)
		}
		// 新 Broker 的组协调器可能晚于 Topic 就绪，只在启动预算内重试临时错误。
		if !kerr.IsRetriable(err) {
			group.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			group.Close()
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Run 启动组内所有成员。任一成员失败就停止同组拉取，等在途处理结束后再统一复查。
func (group *ConsumerGroup) Run(ctx context.Context, process func(context.Context, Record) error, quarantine func(context.Context, FailedRecord) error) error {
	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first sync.Once
	var cause error
	for _, member := range group.members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := member.Run(groupCtx, process, quarantine); err != nil {
				first.Do(func() {
					cause = err
					cancel()
				})
			}
		}()
	}
	wg.Wait()
	return cause
}

// Close 在全组退出后释放各成员及其分区所有权。
func (group *ConsumerGroup) Close() {
	for _, member := range group.members {
		member.Close()
	}
}

// ObserveLag 在组运行期间只查询进度，不改变组位置。
func (group *ConsumerGroup) ObserveLag(ctx context.Context, metrics *Metrics) error {
	if len(group.members) == 0 {
		return errors.New("消费组尚未创建")
	}
	first := group.members[0]
	ends, err := first.logOffsets(ctx, -1)
	if err != nil {
		return err
	}
	positions, err := first.CommittedOffsets(ctx)
	if err != nil {
		return err
	}
	for partition, end := range ends {
		metrics.Lag.WithLabelValues(first.group, strconv.Itoa(int(partition))).Set(float64(max(end-max(positions[partition], 0), 0)))
	}
	return nil
}

// RunObserved 的采样协程随本次组运行创建和回收，不能跨会话复用。
func (group *ConsumerGroup) RunObserved(ctx context.Context, process func(context.Context, Record) error, quarantine func(context.Context, FailedRecord) error, metrics *Metrics) error {
	child, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	name := group.members[0].group
	metrics.Consumer.WithLabelValues(name).Set(1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			check, finish := context.WithTimeout(child, time.Second)
			_ = group.ObserveLag(check, metrics)
			finish()
			select {
			case <-child.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	for _, member := range group.members {
		member.metrics = metrics
	}
	err := group.Run(child, process, quarantine)
	cancel()
	<-done
	metrics.Consumer.WithLabelValues(name).Set(0)
	return err
}

package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
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

// NewConsumer 创建独立消费组，并要求 live.events 已存在且有稳定 Topic UUID。
func NewConsumer(ctx context.Context, brokers []string, group string) (*Consumer, error) {
	if len(brokers) == 0 || (group != "live-realtime" && group != "live-persistence") {
		return nil, errors.New("Kafka 地址或消费组无效")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics("live.events"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
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
func (consumer *Consumer) Run(ctx context.Context, process func(context.Context, *kgo.Record, string) error) error {
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
			if err := process(ctx, record, consumer.topicID); err != nil {
				processingErr = err
				return
			}
			if err := consumer.client.CommitRecords(ctx, record); err != nil {
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
		if len(response.Groups) != 1 || response.Groups[0].ErrorCode != 0 {
			return nil, errors.New("读取 Kafka 消费组位移失败")
		}
		for _, topic := range response.Groups[0].Topics {
			if topic.Topic == "live.events" {
				for _, part := range topic.Partitions {
					if part.ErrorCode != 0 {
						return nil, errors.New("读取 Kafka 分区位移失败")
					}
					result[part.Partition] = part.Offset
				}
			}
		}
	} else {
		if response.ErrorCode != 0 {
			return nil, errors.New("读取 Kafka 消费组位移失败")
		}
		for _, topic := range response.Topics {
			if topic.Topic == "live.events" {
				for _, part := range topic.Partitions {
					if part.ErrorCode != 0 {
						return nil, errors.New("读取 Kafka 分区位移失败")
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

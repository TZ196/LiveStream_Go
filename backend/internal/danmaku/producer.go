package danmaku

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Producer 负责将事件写入 Kafka；不负责业务准入和消费端去重。
type Producer struct{ client *kgo.Client }

// NewProducer 创建使用 Java 兼容分区、全部副本确认和幂等发送的客户端。
func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("至少需要一个 Kafka Broker 地址")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerBatchMaxBytes(48*1024),
		kgo.RecordDeliveryTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka Producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish 等待 Kafka 确认；返回错误不能证明消息没有进入 Kafka。
func (producer *Producer) Publish(ctx context.Context, event Event) error {
	record, err := event.KafkaRecord()
	if err != nil {
		return err
	}
	result := producer.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("发送弹幕事件结果未确认: %w", err)
	}
	return nil
}

// Close 等待未完成的发送并关闭客户端；调用方应先停止接收新请求。
func (producer *Producer) Close() { producer.client.Close() }

// Drain 在停止准入后等待已提交到客户端的发送，包含结果尚未确认的发送。
func (producer *Producer) Drain(ctx context.Context) error { return producer.client.Flush(ctx) }

// Ping 检查 Producer 是否能连接 Kafka Broker。
func (producer *Producer) Ping(ctx context.Context) error { return producer.client.Ping(ctx) }

// HasEvents 只检查日志末端，帮助识别 Redis 请求账本丢失后的旧请求风险。
func (producer *Producer) HasEvents(ctx context.Context) (bool, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.ListOffsetsRequestTopic{Topic: Topic}
	for partition := int32(0); partition < 16; partition++ {
		item := kmsg.NewListOffsetsRequestTopicPartition()
		item.Partition, item.Timestamp = partition, -1
		topic.Partitions = append(topic.Partitions, item)
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	response, err := request.RequestWith(ctx, producer.client)
	if err != nil {
		return false, err
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 16 {
		return false, errors.New("弹幕日志元数据不完整")
	}
	any := false
	for _, item := range response.Topics[0].Partitions {
		if err := kerr.ErrorForCode(item.ErrorCode); err != nil {
			return false, err
		}
		if item.Offset > 0 {
			any = true
		}
	}
	return any, nil
}

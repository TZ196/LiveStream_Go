package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

// PublishLike 等待 Kafka 确认；返回错误不能证明消息没有进入 Kafka。
func (producer *Producer) PublishLike(ctx context.Context, event LikeEvent) error {
	key, value, err := event.KafkaRecord()
	if err != nil {
		return err
	}
	result := producer.client.ProduceSync(ctx, &kgo.Record{Topic: "live.events", Key: key, Value: value})
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("发送点赞事件结果未确认: %w", err)
	}
	return nil
}

// Close 等待未完成的发送并关闭客户端；调用方应先停止接收新请求。
func (producer *Producer) Close() { producer.client.Close() }

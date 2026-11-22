package messaging

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestKafkaProducerIntegration 在独立测试 Broker 中验证重复事件确实以相同键和值写入。
func TestKafkaProducerIntegration(t *testing.T) {
	broker := os.Getenv("LIVESTREAM_TEST_KAFKA")
	if broker == "" {
		t.Skip("未设置独立 Kafka 测试地址")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := NewProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewLikeEvent("1", "1", "1", id.String(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := producer.PublishLike(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("live.events"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	expectedKey, expectedValue, err := event.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for count < 2 {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if bytes.Equal(record.Key, expectedKey) && bytes.Equal(record.Value, expectedValue) {
				count++
			}
		})
	}
}

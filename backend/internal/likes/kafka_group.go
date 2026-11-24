package likes

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

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
func (group *ConsumerGroup) Run(ctx context.Context, process func(context.Context, ConsumedLike) error) error {
	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first sync.Once
	var cause error
	for _, member := range group.members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := member.Run(groupCtx, process); err != nil {
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

package messaging

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestLikeEventRetry 验证重复发送沿用完全相同的事件身份、时间、分区键与消息体。
func TestLikeEventRetry(t *testing.T) {
	now := time.UnixMilli(1794988800001)
	request := "01a1edab-7801-7abc-8000-123456789abc"
	first, err := NewLikeEvent("2001", "1001", "3001", request, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLikeEvent("2001", "1001", "3001", request, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	key1, value1, err := first.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	key2, value2, err := second.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key1, key2) || !bytes.Equal(value1, value2) {
		t.Fatal("重试事件发生变化")
	}
	if first.Timestamp != now.UnixMilli() || first.EventID != "2b986e4f-c298-5d56-b6d8-b7ba5b5b8c66" || first.RequestHash != "34b33ca494a51b00fdf52cb1baf61ed8d19b7637a4e8feab9f9f07ce18bc5493" {
		t.Fatalf("事件不符合文档示例: %+v", first)
	}
	conflict, err := NewLikeEvent("2001", "1002", "3001", request, now)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.EventID != first.EventID || conflict.RequestHash == first.RequestHash {
		t.Fatal("冲突事件应保留 ID 并改变指纹")
	}
}

// TestLikeEventValidation 验证标识及时间窗口，拒绝非规范输入。
func TestLikeEventValidation(t *testing.T) {
	now := time.UnixMilli(1794988800001)
	request := "01a1edab-7801-7abc-8000-123456789abc"
	for _, tc := range []struct {
		user, room, session, request string
		now                          time.Time
	}{
		{"01", "1", "1", request, now}, {"1", "0", "1", request, now}, {"1", "1", "1", "invalid", now},
		{"1", "1", "1", request, now.Add(24*time.Hour + time.Millisecond)}, {"1", "1", "1", request, now.Add(-5*time.Minute - time.Millisecond)},
	} {
		if _, err := NewLikeEvent(tc.user, tc.room, tc.session, tc.request, tc.now); err == nil {
			t.Fatalf("非法输入未拒绝: %+v", tc)
		}
	}
}

// TestLikeBatchIdentity 覆盖旧请求重试、数量指纹冲突和批次记录的反序列化。
func TestLikeBatchIdentity(t *testing.T) {
	now := time.UnixMilli(1794988800001)
	request := "01a1edab-7801-7abc-8000-123456789abc"
	legacy, err := NewLikeEvent("1", "2", "3", request, now)
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := legacy.KafkaRecord()
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{`{"count":0}`, `{"count":1}`, `{"count":null}`, `null`} {
		malformed := bytes.Replace(value, []byte(`"payload":{}`), []byte(`"payload":`+invalid), 1)
		if _, err := DecodeLikeRecord(&kgo.Record{Topic: "live.events", Key: key, Value: malformed}); err == nil {
			t.Fatalf("旧版事件错误接受了载荷 %s", invalid)
		}
	}
	for _, count := range []int64{1, 5, 20000, math.MaxInt64} {
		batch, err := NewLikeBatch("1", "2", "3", request, count, now)
		if err != nil {
			t.Fatal(err)
		}
		if batch.ClickCount() != count || batch.EventID != legacy.EventID || (batch.RequestHash == legacy.RequestHash) != (count == 1) {
			t.Fatalf("批次数量或重试指纹错误：%+v", batch)
		}
		key, value, err := batch.KafkaRecord()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeLikeRecord(&kgo.Record{Topic: "live.events", Key: key, Value: value})
		if err != nil || decoded != batch {
			t.Fatalf("批次解码：%+v, %v", decoded, err)
		}
	}
	if _, err := NewLikeBatch("1", "2", "3", request, 0, now); err == nil {
		t.Fatal("零次点击不能创建批次")
	}
}

package likes

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"strconv"
	"time"
)

// LikeEvent 保存一个不可拆分的点赞批次；版本 1 的历史事件固定为一次点击。
type LikeEvent struct {
	SchemaVersion int         `json:"schema_version"`
	EventID       string      `json:"event_id"`
	EventType     string      `json:"event_type"`
	RequestID     string      `json:"request_id"`
	RequestHash   string      `json:"request_hash"`
	RoomID        string      `json:"room_id"`
	SessionID     string      `json:"session_id"`
	UserID        string      `json:"user_id"`
	Timestamp     int64       `json:"timestamp"`
	Payload       LikePayload `json:"payload"`
}

type LikePayload struct {
	Count int64 `json:"count,omitempty"`
}

// UnmarshalJSON 区分旧格式的空对象与显式 count。显式的零、负数和额外字段均无效。
func (payload *LikePayload) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return errors.New("点赞数据必须为对象")
	}
	if len(fields) == 0 {
		payload.Count = 0
		return nil
	}
	count, err := strconv.ParseInt(string(fields["count"]), 10, 64)
	if len(fields) != 1 || err != nil || count < 1 {
		return errors.New("count 必须为正整数")
	}
	payload.Count = count
	return nil
}

// ClickCount 将旧的空载荷还原成一次点击。
func (event LikeEvent) ClickCount() int64 {
	if event.SchemaVersion == 1 {
		return 1
	}
	return event.Payload.Count
}

// NewLikeEvent 从已认证的用户和已校验的场次构造稳定事件，不进行 Redis 操作。
// 调用方负责验证用户权限、场次归属及开放状态。
func NewLikeEvent(userID, roomID, sessionID, requestID string, now time.Time) (LikeEvent, error) {
	return buildLikeEvent(userID, roomID, sessionID, requestID, 1, 1, now)
}

// NewLikeBatch 冻结本次点击数；同一请求在任何重试中都使用相同的数量与身份。
func NewLikeBatch(userID, roomID, sessionID, requestID string, count int64, now time.Time) (LikeEvent, error) {
	if count < 1 {
		return LikeEvent{}, errors.New("点赞批次数量必须为正整数")
	}
	return buildLikeEvent(userID, roomID, sessionID, requestID, 2, count, now)
}

func buildLikeEvent(userID, roomID, sessionID, requestID string, version int, count int64, now time.Time) (LikeEvent, error) {
	for _, value := range []string{userID, roomID, sessionID} {
		number, err := strconv.ParseUint(value, 10, 64)
		if err != nil || number == 0 || strconv.FormatUint(number, 10) != value {
			return LikeEvent{}, errors.New("用户、房间和场次 ID 必须是规范的正整数字符串")
		}
	}
	id, err := uuid.Parse(requestID)
	if err != nil || id.String() != requestID || id.Version() != 7 || id.Variant() != uuid.RFC4122 {
		return LikeEvent{}, errors.New("request_id 必须是规范小写 UUIDv7")
	}
	millis := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 | int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	eventTime := time.UnixMilli(millis)
	if eventTime.Before(now.Add(-24*time.Hour)) || eventTime.After(now.Add(5*time.Minute)) {
		return LikeEvent{}, errors.New("request_id 时间超出允许窗口")
	}
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("livestream/v2/"+userID+"/"+requestID))
	canonical := "LIKE_CLICKED\n" + roomID + "\n" + sessionID + "\n"
	// 数量为 1 时沿用旧指纹，升级前的待确认请求仍可安全重试。
	if count != 1 {
		canonical += strconv.FormatInt(count, 10) + "\n"
	}
	fingerprint := sha256.Sum256([]byte(canonical))
	var payload LikePayload
	if version == 2 {
		payload.Count = count
	}
	return LikeEvent{SchemaVersion: version, Payload: payload, EventID: eventID.String(), EventType: "LIKE_CLICKED", RequestID: requestID,
		RequestHash: hex.EncodeToString(fingerprint[:]), RoomID: roomID, SessionID: sessionID,
		UserID: userID, Timestamp: millis}, nil
}

// KafkaRecord 返回事件级分区键和 JSON 内容；重复事件的结果保持一致。
func (event LikeEvent) KafkaRecord() ([]byte, []byte, error) {
	value, err := json.Marshal(event)
	if err != nil {
		return nil, nil, fmt.Errorf("序列化点赞事件: %w", err)
	}
	return []byte(event.RoomID + ":" + event.EventID), value, nil
}

// ConsumedLike 是通过记录校验的事件及消费位置。两个仓储共享这个输入，各自完成业务写入。
// 原始 JSON 供 inbox 留档，生产时间只用于送达测量，位移提交由 Consumer 持有原记录执行。
type ConsumedLike struct {
	Event           LikeEvent
	TopicID         string
	Partition       int32
	NextOffset      int64
	RawJSON         []byte
	ProducedAt      time.Time
	HasProducerTime bool
}

// DecodeLikeRecord 校验来自 Kafka 的外壳、事件身份、指纹和分区键。
// 消费积压时不使用当前时间窗口，避免拒绝已经接受的旧事件。
func DecodeLikeRecord(record *kgo.Record, topicID string) (ConsumedLike, error) {
	if record.Topic != "live.events" || len(record.Value) > 64*1024 {
		return ConsumedLike{}, errors.New("事件 topic 或大小无效")
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	var event LikeEvent
	if err := decoder.Decode(&event); err != nil {
		return ConsumedLike{}, fmt.Errorf("事件格式无效: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ConsumedLike{}, errors.New("事件包含多余内容")
	}
	var expected LikeEvent
	var err error
	eventTime := time.UnixMilli(event.Timestamp)
	switch event.SchemaVersion {
	case 1:
		expected, err = NewLikeEvent(event.UserID, event.RoomID, event.SessionID, event.RequestID, eventTime)
	case 2:
		expected, err = NewLikeBatch(event.UserID, event.RoomID, event.SessionID, event.RequestID, event.Payload.Count, eventTime)
	default:
		return ConsumedLike{}, errors.New("未知的点赞事件版本")
	}
	if err != nil {
		return ConsumedLike{}, err
	}
	if event != expected {
		return ConsumedLike{}, errors.New("事件身份、内容或指纹不匹配")
	}
	key, _, err := expected.KafkaRecord()
	if err != nil {
		return ConsumedLike{}, err
	}
	if !bytes.Equal(key, record.Key) {
		return ConsumedLike{}, errors.New("事件分区键不匹配")
	}
	return ConsumedLike{
		Event: event, TopicID: topicID, Partition: record.Partition, NextOffset: record.Offset + 1,
		RawJSON: record.Value, ProducedAt: record.Timestamp, HasProducerTime: record.Attrs.TimestampType() == 0,
	}, nil
}

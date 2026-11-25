package danmaku

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Topic = "live.danmaku"
const RealtimeGroup = "danmaku-realtime"
const PersistenceGroup = "danmaku-persistence"

// Event 是审核通过后冻结的弹幕；发布重试不能更换任何业务字段。
type Event struct {
	SchemaVersion     int     `json:"schema_version"`
	EventID           string  `json:"event_id"`
	EventType         string  `json:"event_type"`
	RequestID         string  `json:"request_id"`
	RequestHash       string  `json:"request_hash"`
	RoomID            string  `json:"room_id"`
	SessionID         string  `json:"session_id"`
	UserID            string  `json:"user_id"`
	Payload           Payload `json:"payload"`
	ModerationVersion string  `json:"moderation_version"`
	Timestamp         int64   `json:"timestamp"`
}

type Payload struct {
	Username string `json:"username"`
	Content  string `json:"content"`
}

// Fault 区分确定拒绝和结果仍未确认；接入层只负责映射协议。
type Fault struct {
	Code       string
	Message    string
	Retryable  bool
	Pending    bool
	Category   string
	RetryAfter int
}

func (fault *Fault) Error() string { return fault.Message }

func failure(code, message string, retryable bool) *Fault {
	return &Fault{Code: code, Message: message, Retryable: retryable, RetryAfter: 1000}
}

var ErrConflict = errors.New("弹幕事件身份或内容冲突")
var ErrBlocked = errors.New("弹幕消费恢复依据不足")
var ErrInvalidEvent = errors.New("弹幕事件违反持久化约束")

func ValidateText(content string) error {
	if !utf8.ValidString(content) || utf8.RuneCountInString(content) < 1 || utf8.RuneCountInString(content) > 200 || strings.TrimSpace(content) == "" {
		return failure("INVALID_ARGUMENT", "弹幕须为 1 至 200 个 Unicode 字符，不能全为空白", false)
	}
	for _, ch := range content {
		if unicode.IsControl(ch) && ch != '\n' && ch != '\t' {
			return failure("INVALID_ARGUMENT", "弹幕包含不允许的控制字符", false)
		}
	}
	return nil
}

// NewRequest 沿用 UUIDv7 请求和 UUIDv5 事件身份，审核版本稍后在通过审核时冻结。
func NewRequest(user, room, session, request, content string, now time.Time) (Event, error) {
	for _, value := range []string{user, room, session} {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != value {
			return Event{}, failure("INVALID_ARGUMENT", "用户、房间和场次编号无效", false)
		}
	}
	id, err := uuid.Parse(request)
	if err != nil || id.String() != request || id.Version() != 7 || id.Variant() != uuid.RFC4122 {
		return Event{}, failure("INVALID_ARGUMENT", "请求编号须为规范小写 UUIDv7", false)
	}
	millis := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 | int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	if time.UnixMilli(millis).After(now.Add(5 * time.Minute)) {
		return Event{}, failure("CLOCK_SKEW", "客户端时间偏差过大", false)
	}
	if err := ValidateText(content); err != nil {
		return Event{}, err
	}
	sum := sha256.Sum256([]byte("DANMAKU_SENT\n" + room + "\n" + session + "\n" + content))
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("livestream/v2/"+user+"/"+request))
	return Event{SchemaVersion: 1, EventID: eventID.String(), EventType: "DANMAKU_SENT", RequestID: request, RequestHash: hex.EncodeToString(sum[:]), RoomID: room, SessionID: session, UserID: user, Timestamp: millis, Payload: Payload{Content: content}}, nil
}

func (event Event) Key() string { return event.RoomID + ":" + event.UserID }

func (event Event) KafkaRecord() (*kgo.Record, error) {
	value, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: Topic, Key: []byte(event.Key()), Value: value, Timestamp: time.Now()}, nil
}

type Record struct {
	Event      Event
	TopicID    string
	Partition  int32
	NextOffset int64
	ProducedAt time.Time
	ReceivedAt time.Time
}

// DecodeRecord 只校验冻结事件，不以当前词库重新裁定已经接受的旧消息。
func DecodeRecord(raw *kgo.Record, topicID string) (Record, error) {
	if raw.Topic != Topic || len(raw.Value) > 64*1024 || !utf8.Valid(raw.Value) {
		return Record{}, errors.New("弹幕 Topic、编码或大小无效")
	}
	var event Event
	decoder := json.NewDecoder(bytes.NewReader(raw.Value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return Record{}, err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return Record{}, errors.New("事件包含多余内容")
	}
	expected, err := NewRequest(event.UserID, event.RoomID, event.SessionID, event.RequestID, event.Payload.Content, time.UnixMilli(event.Timestamp))
	if err != nil {
		return Record{}, err
	}
	version, err := hex.DecodeString(event.ModerationVersion)
	if err != nil || len(version) != 32 || strings.ToLower(event.ModerationVersion) != event.ModerationVersion {
		return Record{}, errors.New("缺少有效的冻结审核版本")
	}
	if event.Payload.Username == "" || len(event.Payload.Username) > 32 || !utf8.ValidString(event.Payload.Username) {
		return Record{}, errors.New("用户名快照无效")
	}
	if event.SchemaVersion != 1 || event.EventType != "DANMAKU_SENT" || event.EventID != expected.EventID || event.RequestHash != expected.RequestHash || event.Timestamp != expected.Timestamp || string(raw.Key) != event.Key() {
		return Record{}, fmt.Errorf("%w: 事件外壳、时间、指纹或分区键不匹配", ErrConflict)
	}
	var received time.Time
	for _, header := range raw.Headers {
		if header.Key == "api_received_at_ns" {
			if nanos, err := strconv.ParseInt(string(header.Value), 10, 64); err == nil && nanos > 0 {
				received = time.Unix(0, nanos)
			}
		}
	}
	return Record{ReceivedAt: received, Event: event, TopicID: topicID, Partition: raw.Partition, NextOffset: raw.Offset + 1, ProducedAt: raw.Timestamp}, nil
}

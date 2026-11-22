package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// LikeEvent 是发送给 Kafka 的单次点赞事件；一次事件固定表示一次点击。
type LikeEvent struct {
	SchemaVersion int      `json:"schema_version"`
	EventID       string   `json:"event_id"`
	EventType     string   `json:"event_type"`
	RequestID     string   `json:"request_id"`
	RequestHash   string   `json:"request_hash"`
	RoomID        string   `json:"room_id"`
	SessionID     string   `json:"session_id"`
	UserID        string   `json:"user_id"`
	Timestamp     int64    `json:"timestamp"`
	Payload       struct{} `json:"payload"`
}

// NewLikeEvent 从已认证的用户和已校验的场次构造稳定事件，不进行 Redis 操作。
// 调用方负责验证用户权限、场次归属及开放状态。
func NewLikeEvent(userID, roomID, sessionID, requestID string, now time.Time) (LikeEvent, error) {
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
	fingerprint := sha256.Sum256([]byte("LIKE_CLICKED\n" + roomID + "\n" + sessionID + "\n"))
	return LikeEvent{SchemaVersion: 1, EventID: eventID.String(), EventType: "LIKE_CLICKED", RequestID: requestID,
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

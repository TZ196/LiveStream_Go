package danmaku

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FailedRecord 保留无法解码的原始 Kafka 位置；不能依赖 Event 中可能缺失的字段。
type FailedRecord struct {
	Raw      *kgo.Record
	TopicID  string
	Group    string
	Code     string
	Cause    error
	Attempts int
}

// FailedEvent 是可追查的异常记录，不是已成功持久化的正常弹幕。
type FailedEvent struct {
	ConsumerGroup     string `gorm:"primaryKey"`
	TopicID           string `gorm:"primaryKey"`
	PartitionID       int32  `gorm:"primaryKey;autoIncrement:false"`
	RecordOffset      int64  `gorm:"primaryKey;autoIncrement:false"`
	RawKey            []byte
	RawPayload        []byte
	KeyHash           string
	PayloadHash       string
	KeyBytes          uint64
	PayloadBytes      uint64
	Truncated         bool
	ErrorCode         string
	ErrorMessage      string
	Attempts          int
	Status            string
	ResolutionDetails string
	FirstFailedAt     time.Time `gorm:"type:datetime(3);default:CURRENT_TIMESTAMP(3)"`
	ResolvedAt        *time.Time
}

func (FailedEvent) TableName() string { return "failed_events" }

// Quarantine 在可靠隔离后才允许消费循环提交。只有 Worker 组推进数据库检查点。
// 两个消费组分别登记，避免一个组的隔离状态覆盖另一个组的处理结果。
func (repository *Repository) Quarantine(ctx context.Context, failed FailedRecord) error {
	raw := failed.Raw
	if raw == nil {
		return errors.New("缺少弹幕异常记录")
	}
	invalidPartition := raw.Partition < 0 || raw.Partition >= 16
	invalidOffset := raw.Offset < 0 || raw.Offset == 1<<63-1
	if raw.Topic != Topic || invalidPartition || invalidOffset {
		return errors.New("缺少有效的弹幕异常来源位置")
	}
	topic, err := uuid.Parse(failed.TopicID)
	if err != nil || topic.String() != failed.TopicID || topic == uuid.Nil {
		return errors.New("异常来源缺少有效的 Topic 身份")
	}
	if failed.Group != RealtimeGroup && failed.Group != PersistenceGroup {
		return errors.New("弹幕异常消费组无效")
	}
	if failed.Cause == nil || failed.Attempts < 1 || failed.Attempts > 3 {
		return errors.New("异常来源的原因或尝试次数无效")
	}
	if failed.Code != "INVALID_EVENT" && failed.Code != "EVENT_CONFLICT" {
		return errors.New("只有确定的永久事件错误可以隔离")
	}
	keyHash := sha256.Sum256(raw.Key)
	payloadHash := sha256.Sum256(raw.Value)
	message := []rune(strings.ToValidUTF8(failed.Cause.Error(), "�"))
	if len(message) > 512 {
		message = message[:512]
	}
	row := FailedEvent{
		ConsumerGroup: failed.Group,
		TopicID:       failed.TopicID,
		PartitionID:   raw.Partition,
		RecordOffset:  raw.Offset,
		RawKey:        retainRaw(raw.Key),
		RawPayload:    retainRaw(raw.Value),
		KeyHash:       hex.EncodeToString(keyHash[:]),
		PayloadHash:   hex.EncodeToString(payloadHash[:]),
		KeyBytes:      uint64(len(raw.Key)),
		PayloadBytes:  uint64(len(raw.Value)),
		Truncated:     len(raw.Key) > 64*1024 || len(raw.Value) > 64*1024,
		ErrorCode:     failed.Code,
		ErrorMessage:  string(message),
		Attempts:      failed.Attempts,
		Status:        "OPEN",
	}
	return repository.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 重放不能覆盖原始内容或人工处置结果，也不能使用 INSERT IGNORE 吞掉其他 SQL 错误。
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
			return err
		}
		var existing FailedEvent
		query := tx.Where(
			"consumer_group = ? AND topic_id = ? AND partition_id = ? AND record_offset = ?",
			row.ConsumerGroup, row.TopicID, row.PartitionID, row.RecordOffset,
		)
		if err := query.Take(&existing).Error; err != nil {
			return err
		}
		sameHashes := existing.KeyHash == row.KeyHash && existing.PayloadHash == row.PayloadHash
		sameSizes := existing.KeyBytes == row.KeyBytes && existing.PayloadBytes == row.PayloadBytes
		sameRaw := bytes.Equal(existing.RawKey, row.RawKey) && bytes.Equal(existing.RawPayload, row.RawPayload)
		if !sameHashes || !sameSizes || !sameRaw {
			return ErrBlocked
		}
		if failed.Group == PersistenceGroup {
			return advanceCheckpoint(tx, failed.TopicID, raw.Partition, raw.Offset+1)
		}
		return nil
	})
}

// retainRaw 对异常输入也限制存储量；超长值另存原长度和完整哈希，不伪称保留了完整正文。
func retainRaw(raw []byte) []byte {
	return append([]byte{}, raw[:min(len(raw), 64*1024)]...)
}

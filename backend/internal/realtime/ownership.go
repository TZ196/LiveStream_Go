package realtime

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	ownerLease         = 30 * time.Second
	ownerRenewInterval = 10 * time.Second
	ownerRedisTimeout  = 600 * time.Millisecond
	ownerRenewBatch    = 64
)

var (
	ErrInvalidWatch         = errors.New("观看请求无效")
	ErrWatchReplaced        = errors.New("观看已被其他窗口替换")
	ErrWatchExpired         = errors.New("观看已失效")
	ErrOwnershipUnavailable = errors.New("直播连接登记暂不可用")
	ErrTicketCapacity       = errors.New("票据容量已满")
)

//go:embed lua/claim_owner.lua
var claimOwnerLua string

//go:embed lua/renew_owner.lua
var renewOwnerLua string

//go:embed lua/release_owner.lua
var releaseOwnerLua string

var (
	claimOwner   = redis.NewScript(claimOwnerLua)
	renewOwner   = redis.NewScript(renewOwnerLua)
	releaseOwner = redis.NewScript(releaseOwnerLua)
)

// ownershipRecord 只存用于识别连接的字段，实际 Socket 由本地房间持有。
type ownershipRecord struct {
	UserID       string `json:"user_id"`
	InstanceID   string `json:"instance_id"`
	ConnectionID string `json:"connection_id"`
	RoomID       string `json:"room_id"`
	SessionID    string `json:"session_id"`
	WatchID      string `json:"watch_id"`
	Generation   int64  `json:"generation"`
	RunID        string `json:"run_id"`
	Active       bool   `json:"active"`
}

type watchIntent struct {
	mode, watchID, expectedWatch, expectedConnection, runID string
}

type ownershipStore struct{ redis *redis.Client }

func ownerKey(userID string) string           { return "ws:owner:{" + userID + "}" }
func replaceChannel(instanceID string) string { return "ws:replace:" + instanceID }

func (s *ownershipStore) inspect(parent context.Context, userID string) (ownershipRecord, string, error) {
	ctx, cancel := context.WithTimeout(parent, ownerRedisTimeout)
	defer cancel()
	pipe := s.redis.Pipeline()
	info := pipe.Info(ctx, "server")
	owner := pipe.Get(ctx, ownerKey(userID))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return ownershipRecord{}, "", ErrOwnershipUnavailable
	}
	server, err := info.Result()
	if err != nil {
		return ownershipRecord{}, "", ErrOwnershipUnavailable
	}
	var runID string
	for _, line := range strings.Split(server, "\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			runID = strings.TrimSpace(value)
			break
		}
	}
	if runID == "" {
		return ownershipRecord{}, "", ErrOwnershipUnavailable
	}
	raw, err := owner.Bytes()
	if errors.Is(err, redis.Nil) {
		return ownershipRecord{}, runID, nil
	}
	var record ownershipRecord
	if err != nil || json.Unmarshal(raw, &record) != nil {
		return ownershipRecord{}, "", ErrOwnershipUnavailable
	}
	if record.RunID != runID {
		return ownershipRecord{}, runID, nil
	}
	return record, runID, nil
}

// IssueTicket 只读取归属并冻结观看意图，成功消费票据并升级后才会改写 Redis。
func (m *Manager) IssueTicket(ctx context.Context, identity user.Identity, roomID, sessionID uint64, origin, mode, watchID string) (string, time.Time, string, error) {
	if mode == "resume" {
		id, err := uuid.Parse(watchID)
		if err != nil || id.Version() != 4 || id.String() != watchID {
			return "", time.Time{}, "", ErrInvalidWatch
		}
	} else if mode != "enter" || watchID != "" {
		return "", time.Time{}, "", ErrInvalidWatch
	}
	if m.ownership == nil {
		return "", time.Time{}, "", ErrOwnershipUnavailable
	}
	userID := strconv.FormatUint(identity.UserID, 10)
	current, runID, err := m.ownership.inspect(ctx, userID)
	if err != nil {
		return "", time.Time{}, "", err
	}
	if mode == "resume" {
		if current.WatchID == "" {
			return "", time.Time{}, "", ErrWatchExpired
		}
		if current.WatchID != watchID {
			return "", time.Time{}, "", ErrWatchReplaced
		}
		if current.RoomID != strconv.FormatUint(roomID, 10) || current.SessionID != strconv.FormatUint(sessionID, 10) {
			return "", time.Time{}, "", ErrWatchExpired
		}
	} else {
		id, err := uuid.NewRandom()
		if err != nil {
			return "", time.Time{}, "", ErrOwnershipUnavailable
		}
		watchID = id.String()
	}
	intent := watchIntent{mode: mode, watchID: watchID, expectedWatch: current.WatchID, expectedConnection: current.ConnectionID, runID: runID}
	encoded, expires, err := m.Tickets.Issue(identity, roomID, sessionID, origin, intent)
	return encoded, expires, watchID, err
}

func (s *ownershipStore) claim(parent context.Context, next ownershipRecord, proof ticket) (ownershipRecord, ownershipRecord, time.Time, error) {
	started := time.Now()
	deadline := started.Add(ownerLease)
	if proof.identity.ExpiresAt.Before(deadline) {
		deadline = proof.identity.ExpiresAt
	}
	value, err := json.Marshal(next)
	if err != nil {
		return ownershipRecord{}, ownershipRecord{}, time.Time{}, ErrOwnershipUnavailable
	}
	ctx, cancel := context.WithTimeout(parent, ownerRedisTimeout)
	result, err := claimOwner.Run(ctx, s.redis, []string{ownerKey(next.UserID)}, value,
		proof.watch.mode, proof.watch.expectedWatch, proof.expiresAt.UnixMilli(), proof.watch.expectedConnection, ownerLease.Milliseconds()).StringSlice()
	cancel()
	if err != nil {
		// 结果未知时只核对本次连接，不能用另一次主动进入来重试写入。
		current, _, readErr := s.inspect(parent, next.UserID)
		if readErr == nil && current.Active && current.ConnectionID == next.ConnectionID && current.RunID == next.RunID {
			return current, ownershipRecord{}, deadline, nil
		}
		return ownershipRecord{}, ownershipRecord{}, time.Time{}, ErrOwnershipUnavailable
	}
	if len(result) == 1 {
		if result[0] == "replaced" {
			return ownershipRecord{}, ownershipRecord{}, time.Time{}, ErrWatchReplaced
		}
		if result[0] == "expired" {
			return ownershipRecord{}, ownershipRecord{}, time.Time{}, ErrWatchExpired
		}
	}
	var current, previous ownershipRecord
	if len(result) != 3 || result[0] != "ok" || json.Unmarshal([]byte(result[1]), &current) != nil {
		return current, previous, time.Time{}, ErrOwnershipUnavailable
	}
	if result[2] != "" && json.Unmarshal([]byte(result[2]), &previous) != nil {
		return current, previous, time.Time{}, ErrOwnershipUnavailable
	}
	return current, previous, deadline, nil
}

func (s *ownershipStore) release(owner ownershipRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), ownerRedisTimeout)
	defer cancel()
	// 失败时靠原租约过期回收，清理路径不会为失联的 Redis 长时间占住名额。
	_ = releaseOwner.Run(ctx, s.redis, []string{ownerKey(owner.UserID)}, owner.RunID,
		owner.InstanceID, owner.ConnectionID, owner.Generation, ownerLease.Milliseconds()).Err()
}

func (s *ownershipStore) notifyReplacement(parent context.Context, previous ownershipRecord) {
	if !previous.Active {
		return
	}
	ctx, cancel := context.WithTimeout(parent, ownerRedisTimeout)
	defer cancel()
	message, err := json.Marshal(previous)
	if err == nil {
		_ = s.redis.Publish(ctx, replaceChannel(previous.InstanceID), message).Err()
	}
}

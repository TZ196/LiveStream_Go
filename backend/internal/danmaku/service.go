package danmaku

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/room"
	"github.com/TZ196/LiveStream_Go/backend/internal/user"
	"github.com/google/uuid"
)

// Publisher 将业务提交边界限制在 Broker 确认，传输错误不能解释成确定拒绝。
type receivedAtKey struct{}

type Publisher interface {
	Publish(context.Context, Event) error
}
type Service struct {
	Store      *RequestStore
	Repository *Repository
	Moderator  *Moderator
	publisher  Publisher
	rooms      *room.Service
	users      *user.Repository
	healthy    atomic.Bool
	consuming  atomic.Bool
	Metrics    *Metrics
}

func NewService(store *RequestStore, repository *Repository, moderator *Moderator, publisher Publisher, rooms *room.Service, users *user.Repository) *Service {
	return &Service{Store: store, Repository: repository, Moderator: moderator, publisher: publisher, rooms: rooms, users: users, Metrics: NewMetrics()}
}
func (service *Service) SetHealthy(value bool)   { service.healthy.Store(value) }
func (service *Service) SetConsuming(value bool) { service.consuming.Store(value) }
func (service *Service) Available() bool {
	return service.Moderator != nil && service.healthy.Load() && service.consuming.Load() && service.Store.NewAllowed()
}

// storedEvent 再次验证 Redis 冻结记录，不能仅根据 status 就为不同内容返回 ACK。
func storedEvent(raw string, candidate Event) (Event, error) {
	var event Event
	if json.Unmarshal([]byte(raw), &event) != nil {
		return Event{}, ErrConflict
	}
	record, err := event.KafkaRecord()
	if err != nil {
		return Event{}, err
	}
	if _, err = DecodeRecord(record, "request-cache"); err != nil {
		return Event{}, err
	}
	if event.EventID != candidate.EventID || event.RequestHash != candidate.RequestHash || event.UserID != candidate.UserID {
		return Event{}, ErrConflict
	}
	return event, nil
}

// Submit 先恢复原请求结果，再对新请求执行场次准入、限流、审核和冻结。
func (service *Service) Submit(ctx context.Context, userID, roomID, sessionID, requestID, content string) (Event, error) {
	started := time.Now()
	ctx = context.WithValue(ctx, receivedAtKey{}, started)
	defer func() { service.Metrics.Submit.Observe(time.Since(started).Seconds()) }()
	candidate, err := NewRequest(userID, roomID, sessionID, requestID, content, time.Now())
	if err != nil {
		return Event{}, err
	}
	state, lookupErr := service.Store.Lookup(ctx, userID, requestID)
	if lookupErr == nil && state.Hash != "" {
		if state.Hash != candidate.RequestHash {
			return Event{}, failure("REQUEST_CONFLICT", "相同请求编号对应不同内容或场次", false)
		}
		switch state.Status {
		case "ACCEPTED":
			event, err := storedEvent(state.Event, candidate)
			if err != nil {
				return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "原请求记录需要核对", Pending: true, Retryable: true, RetryAfter: 2000}
			}
			service.Metrics.Results.WithLabelValues("accepted_retry").Inc()
			return event, nil
		case "REJECTED":
			return Event{}, &Fault{Code: "CONTENT_BLOCKED", Message: "内容未通过审核，请修改后作为新请求发送", Category: state.Category}
		}
	} else {
		// Redis 缺失不能证明请求没有发生；可靠历史命中时直接返回原接受结果。
		old, found, findErr := service.Repository.FindRequest(ctx, userID, requestID)
		if findErr != nil {
			return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "暂时无法核对弹幕记录", Pending: true, Retryable: true, RetryAfter: 2000}
		}
		if found {
			if old.EventID != candidate.EventID || old.RequestHash != candidate.RequestHash {
				return Event{}, failure("REQUEST_CONFLICT", "相同请求编号对应不同内容或场次", false)
			}
			return old, nil
		}
	}
	if state.Hash == "" && (lookupErr != nil || service.Store.Uncertain(candidate.Timestamp)) {
		return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "旧请求结果仍待核对，请保留原记录", Pending: true, Retryable: true, RetryAfter: 2000}
	}
	frozen := state.Event != ""
	if !frozen && !service.Available() {
		return Event{}, failure("DANMAKU_UNAVAILABLE", "弹幕暂不可用，请保留草稿稍后重试", true)
	}
	if time.Since(time.UnixMilli(candidate.Timestamp)) > 24*time.Hour {
		return Event{}, &Fault{Code: "REQUEST_EXPIRED", Message: "原请求已超过重试期限，结果仍待核对", Pending: true}
	}
	roomNumber, _ := strconv.ParseUint(roomID, 10, 64)
	sessionNumber, _ := strconv.ParseUint(sessionID, 10, 64)
	active, err := service.rooms.ActiveSession(ctx, roomNumber)
	if err != nil || active.ID != sessionNumber {
		if frozen {
			return Event{}, &Fault{Code: "ROOM_CLOSED", Message: "原场次已结束，原弹幕结果仍待核对，请查询历史", Pending: true}
		}
		return Event{}, failure("ROOM_CLOSED", "原直播场次已结束，不能转发到新场次", false)
	}
	owner := uuid.NewString()
	beforeLimit := time.Now()
	claim, err := service.Store.Begin(ctx, candidate, owner)
	service.Metrics.Limit.Observe(time.Since(beforeLimit).Seconds())
	if err != nil || len(claim) != 4 {
		if frozen {
			return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "暂时无法核对原请求", Pending: true, Retryable: true, RetryAfter: 2000}
		}
		return Event{}, failure("DANMAKU_UNAVAILABLE", "弹幕请求登记暂不可用", true)
	}
	switch claim[0] {
	case "ACCEPTED":
		return storedEvent(claim[1], candidate)
	case "REJECTED":
		return Event{}, &Fault{Code: "CONTENT_BLOCKED", Message: "内容未通过审核，请修改后发送", Category: claim[2]}
	case "CONFLICT":
		return Event{}, failure("REQUEST_CONFLICT", "相同请求编号对应不同内容或场次", false)
	case "LIMITED":
		return Event{}, &Fault{Code: "RATE_LIMITED", Message: "你在本房间每 10 秒最多发送 10 条新弹幕", Retryable: true, RetryAfter: 10000}
	case "FULL":
		return Event{}, failure("SERVER_BUSY", "弹幕请求容量已满，请稍后重试", true)
	case "BUSY":
		return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "原请求正在处理，请稍后核对", Pending: true, Retryable: true, RetryAfter: 1000}
	case "UNKNOWN", "EXPIRED", "RESET":
		return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "原请求记录不完整，结果仍待核对", Pending: true, Retryable: true, RetryAfter: 2000}
	case "OWNED":
	default:
		return Event{}, failure("DANMAKU_UNAVAILABLE", "弹幕请求状态无效", true)
	}
	defer func() {
		finish, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Store.Change(finish, candidate, owner, "release", "", "")
	}()
	event := candidate
	if claim[1] != "" {
		event, err = storedEvent(claim[1], candidate)
		if err != nil {
			return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "冻结事件需要核对", Pending: true, Retryable: true, RetryAfter: 2000}
		}
	} else {
		if service.Moderator == nil {
			return Event{}, failure("DANMAKU_UNAVAILABLE", "审核词库不可用", true)
		}
		beforeReview := time.Now()
		matches := service.Moderator.Check(content)
		service.Metrics.Review.Observe(time.Since(beforeReview).Seconds())
		if len(matches) > 0 {
			if err := service.Store.Change(ctx, candidate, owner, "reject", matches[0].Category, service.Moderator.Version); err != nil {
				return Event{}, failure("DANMAKU_UNAVAILABLE", "审核结果暂未保存，请稍后核对", true)
			}
			service.Metrics.Results.WithLabelValues("blocked").Inc()
			return Event{}, &Fault{Code: "CONTENT_BLOCKED", Message: "内容未通过审核，请修改后发送", Category: matches[0].Category}
		}
		number, _ := strconv.ParseUint(userID, 10, 64)
		account, err := service.users.FindByID(ctx, number)
		if err != nil {
			return Event{}, failure("DANMAKU_UNAVAILABLE", "无法读取发送者信息", true)
		}
		event.Payload.Username = account.Username
		event.ModerationVersion = service.Moderator.Version
		raw, _ := json.Marshal(event)
		if err := service.Store.Change(ctx, event, owner, "freeze", string(raw), event.ModerationVersion); err != nil {
			return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "冻结请求正在核对", Pending: true, Retryable: true, RetryAfter: 1000}
		}
	}
	raw, _ := json.Marshal(event)
	beforePublish := time.Now()
	err = service.publisher.Publish(ctx, event)
	service.Metrics.Publish.Observe(time.Since(beforePublish).Seconds())
	// 发布结果和保存状态分别处理。清理使用独立短预算，不因请求取消泄漏执行权。
	finish, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err != nil {
		_ = service.Store.Change(finish, event, owner, "release", "", "")
		service.Metrics.Results.WithLabelValues("unconfirmed").Inc()
		return Event{}, &Fault{Code: "REQUEST_IN_PROGRESS", Message: "弹幕结果尚未确认，请保留原请求", Pending: true, Retryable: true, RetryAfter: 1000}
	}
	if err := service.Store.Change(finish, event, owner, "accept", string(raw), event.ModerationVersion); err != nil {
		service.Metrics.Results.WithLabelValues("accepted_cache_failed").Inc()
	}
	service.Metrics.Results.WithLabelValues("accepted").Inc()
	return event, nil
}

func AsFault(err error) *Fault {
	var fault *Fault
	if errors.As(err, &fault) {
		return fault
	}
	return failure("DANMAKU_UNAVAILABLE", "弹幕服务暂不可用", true)
}

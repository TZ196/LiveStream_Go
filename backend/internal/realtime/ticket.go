package realtime

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/user"
)

// ticket 绑定认证身份和唯一场次，原始票据不会被保存。
type ticket struct {
	identity  user.Identity
	roomID    uint64
	sessionID uint64
	origin    string
	expiresAt time.Time
	watch     watchIntent
}

// Tickets 存储单进程短期票据，并限制尚未使用的票据数量。
type Tickets struct {
	mu    sync.Mutex
	items map[[32]byte]ticket
}

// NewTickets 创建内存票据仓库；进程重启会使旧票据失效。
func NewTickets() *Tickets { return &Tickets{items: make(map[[32]byte]ticket)} }

// Issue 生成 30 秒有效的一次性票据，并绑定来源和身份有效期。
func (tickets *Tickets) Issue(identity user.Identity, roomID, sessionID uint64, origin string, watch watchIntent) (string, time.Time, error) {
	now := time.Now()
	if identity.UserID == 0 || roomID == 0 || sessionID == 0 || origin == "" || !identity.ExpiresAt.After(now) {
		return "", time.Time{}, errors.New("票据绑定信息无效")
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", time.Time{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw[:])
	key := sha256.Sum256([]byte(encoded))
	expires := now.Add(30 * time.Second)
	if identity.ExpiresAt.Before(expires) {
		expires = identity.ExpiresAt
	}
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	for existing, value := range tickets.items {
		if !value.expiresAt.After(now) {
			delete(tickets.items, existing)
		}
	}
	if len(tickets.items) >= 10000 {
		return "", time.Time{}, ErrTicketCapacity
	}
	tickets.items[key] = ticket{identity: identity, roomID: roomID, sessionID: sessionID, origin: origin, expiresAt: expires, watch: watch}
	return encoded, expires, nil
}

// consume 原子消费票据，拒绝来源、房间或时间不匹配的请求。
func (tickets *Tickets) consume(encoded string, roomID uint64, origin string) (ticket, error) {
	if len(encoded) != 43 {
		return ticket{}, errors.New("票据无效")
	}
	key := sha256.Sum256([]byte(encoded))
	tickets.mu.Lock()
	defer tickets.mu.Unlock()
	value, ok := tickets.items[key]
	if !ok {
		return ticket{}, errors.New("票据无效或已使用")
	}
	delete(tickets.items, key)
	if value.roomID != roomID || value.origin != origin || !value.expiresAt.After(time.Now()) || !value.identity.ExpiresAt.After(time.Now()) {
		return ticket{}, errors.New("票据已过期或绑定不符")
	}
	return value, nil
}

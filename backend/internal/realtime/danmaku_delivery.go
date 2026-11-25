package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
)

type danmakuDelivery struct {
	record danmaku.Record
	ctx    context.Context
	result chan error
}

// QueueDanmaku 等到有界串行分发完成入队才返回成功，不等待浏览器确认。
func (manager *Manager) QueueDanmaku(ctx context.Context, record danmaku.Record) error {
	sessionID, err := strconv.ParseUint(record.Event.SessionID, 10, 64)
	if err != nil {
		return err
	}
	view := manager.lookup(sessionID)
	if view == nil || view.closed.Load() {
		return nil
	}
	if strconv.FormatUint(view.roomID, 10) != record.Event.RoomID {
		return danmaku.ErrConflict
	}
	item := danmakuDelivery{record: record, ctx: ctx, result: make(chan error, 1)}
	select {
	case view.dispatch <- item:
	case <-ctx.Done():
		return ctx.Err()
	case <-view.done:
		return nil
	case <-manager.dispatchCtx.Done():
		return manager.dispatchCtx.Err()
	}
	select {
	case err := <-item.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-view.done:
		return nil
	case <-manager.dispatchCtx.Done():
		return manager.dispatchCtx.Err()
	}
}

func (manager *Manager) dispatchDanmaku(view *audience) {
	defer manager.dispatchWait.Done()
	for {
		select {
		case <-view.done:
			return
		case <-manager.dispatchCtx.Done():
			return
		case item := <-view.dispatch:
			item.result <- manager.fanoutDanmaku(view, item)
		}
	}
}

func (manager *Manager) fanoutDanmaku(view *audience, item danmakuDelivery) error {
	if err := item.ctx.Err(); err != nil {
		return err
	}
	event := item.record.Event
	if previous, exists := view.recent[event.EventID]; exists {
		if previous != event {
			return danmaku.ErrConflict
		}
		return nil
	}
	if err := view.mu.LockContext(item.ctx); err != nil {
		return err
	}
	clients := copyClients(view.clients)
	closed := view.closed.Load()
	view.mu.Unlock()
	if closed {
		return nil
	}
	raw, _ := json.Marshal(map[string]any{"version": 1, "type": "DANMAKU_BROADCAST", "event_id": event.EventID, "request_id": event.RequestID, "data": map[string]any{
		"room_id": event.RoomID, "session_id": event.SessionID, "user_id": event.UserID, "username": event.Payload.Username, "content": event.Payload.Content,
		"created_at": time.UnixMilli(event.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z"),
	}})
	for _, client := range clients {
		client.delivery.Lock()
		if !view.closed.Load() {
			client.offer(&outbound{payload: raw})
		}
		client.delivery.Unlock()
	}
	view.recent[event.EventID] = event
	view.recentOrder = append(view.recentOrder, event.EventID)
	if len(view.recentOrder) > 5000 {
		delete(view.recent, view.recentOrder[0])
		view.recentOrder = view.recentOrder[1:]
	}
	service := manager.danmaku.Load()
	if service != nil && !item.record.ProducedAt.IsZero() && time.Now().After(item.record.ProducedAt) {
		service.Metrics.Enqueue.Observe(time.Since(item.record.ProducedAt).Seconds())
	}
	if service != nil && !item.record.ReceivedAt.IsZero() && time.Now().After(item.record.ReceivedAt) {
		service.Metrics.ReceiveEnqueue.Observe(time.Since(item.record.ReceivedAt).Seconds())
	}
	return nil
}

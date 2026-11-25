package realtime

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
)

// EnableDanmaku 在启动监听前挂接弹幕能力；实际业务由弹幕 Service 负责。
func (manager *Manager) EnableDanmaku(service *danmaku.Service) { manager.danmaku.Store(service) }

func (manager *Manager) receiveDanmaku(client *connection, proof ticket, input frame) {
	service := manager.danmaku.Load()
	if service == nil {
		manager.danmakuError(client, input.RequestID, &danmaku.Fault{Code: "CAPABILITY_NOT_ENABLED", Message: "弹幕功能尚未启用"})
		return
	}
	var data struct {
		Content string `json:"content"`
	}
	if json.Unmarshal(input.Data, &data) != nil {
		manager.danmakuError(client, input.RequestID, &danmaku.Fault{Code: "INVALID_ARGUMENT", Message: "弹幕内容格式无效"})
		return
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(input.Data, &fields) != nil || len(fields) != 1 || fields["content"] == nil {
		manager.danmakuError(client, input.RequestID, &danmaku.Fault{Code: "INVALID_ARGUMENT", Message: "弹幕只接受 content 字段"})
		return
	}
	ctx, cancel := context.WithTimeout(manager.dispatchCtx, 7*time.Second)
	event, err := service.Submit(ctx, strconv.FormatUint(proof.identity.UserID, 10), strconv.FormatUint(proof.roomID, 10), input.SessionID, input.RequestID, data.Content)
	cancel()
	if err != nil {
		manager.danmakuError(client, input.RequestID, danmaku.AsFault(err))
		return
	}
	raw, _ := json.Marshal(map[string]any{"version": 1, "type": "ACK", "request_id": event.RequestID, "event_id": event.EventID, "data": map[string]any{"status": "accepted", "accepted_via": "kafka", "accepted_at": nil}})
	client.enqueue(raw)
}

func (manager *Manager) danmakuError(client *connection, request string, fault *danmaku.Fault) {
	outcome := "rejected"
	if fault.Pending {
		outcome = "pending"
	}
	raw, _ := json.Marshal(map[string]any{"version": 1, "type": "ERROR", "request_id": request, "event_id": nil, "data": map[string]any{
		"code": fault.Code, "message": fault.Message, "category": fault.Category, "outcome": outcome, "retryable": fault.Retryable, "retry_after_ms": fault.RetryAfter,
	}})
	client.enqueue(raw)
}

func (manager *Manager) DanmakuState() string {
	service := manager.danmaku.Load()
	if service == nil {
		return "DISABLED"
	}
	if service.Available() {
		return "READY"
	}
	return "UNAVAILABLE"
}

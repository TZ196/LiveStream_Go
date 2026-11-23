package realtime

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

// 一条采样连接只保留一个待回执。开始时刻包含单调时钟，服务端往返不依赖浏览器校时。
type deliveryProbe struct {
	enabled    atomic.Bool
	pending    atomic.Pointer[time.Time]
	lastIssued time.Time // 只由该连接的写协程访问
}

func (c *connection) stampSample(msg *outbound) []byte {
	p := c.probe
	now := time.Now()
	if !p.enabled.Load() || now.Sub(p.lastIssued) < 30*time.Second {
		return msg.payload
	}
	c.expireProbe(now, false)
	data, err := json.Marshal(likeFrame{Version: 1, Type: "LIKE_UPDATE", Data: msg.total, ServerSentAtMS: now.UnixMilli()})
	if err != nil {
		return msg.payload
	}
	p.lastIssued = now
	p.pending.Store(&now)
	c.metrics.probes.WithLabelValues("issued").Inc()
	return data
}

func (c *connection) expireProbe(now time.Time, closing bool) {
	p := c.probe.pending.Load()
	if p != nil && (closing || now.Sub(*p) > 15*time.Second) && c.probe.pending.CompareAndSwap(p, nil) {
		c.metrics.probes.WithLabelValues("unanswered").Inc()
	}
}

// 控制帧只采样当前连接，由读协程处理。重复或过期回执不参与时延计算。
func (c *connection) handleDeliveryControl(input frame, lastPing *time.Time) {
	if c.probe == nil || input.RequestID != "" {
		return
	}
	now := time.Now()
	if input.Type == "PING" {
		if now.Sub(*lastPing) < 5*time.Second {
			return
		}
		var ping struct {
			Nonce string `json:"nonce"`
		}
		if json.Unmarshal(input.Data, &ping) != nil || len(ping.Nonce) == 0 || len(ping.Nonce) > 64 {
			return
		}
		*lastPing = now
		c.probe.enabled.Store(true)
		pong, _ := encodeFrame("PONG", map[string]any{"nonce": ping.Nonce, "server_time_ms": now.UnixMilli()})
		c.enqueue(pong)
		return
	}
	var receipt struct {
		Sent        int64  `json:"server_sent_at_ms"`
		Received    *int64 `json:"received_at_ms"`
		Uncertainty *int64 `json:"uncertainty_ms"`
	}
	sent := c.probe.pending.Load()
	if sent == nil || json.Unmarshal(input.Data, &receipt) != nil || receipt.Sent != sent.UnixMilli() {
		return
	}
	if !c.probe.pending.CompareAndSwap(sent, nil) {
		return
	}
	rtt := now.Sub(*sent)
	if rtt > 15*time.Second {
		c.metrics.probes.WithLabelValues("unanswered").Inc()
		return
	}
	c.metrics.roundTrip.Observe(rtt.Seconds())
	if receipt.Received == nil || receipt.Uncertainty == nil {
		c.metrics.probes.WithLabelValues("uncalibrated").Inc()
		return
	}
	uncertainty := *receipt.Uncertainty
	if uncertainty < 0 || uncertainty > 60000 {
		c.metrics.probes.WithLabelValues("invalid_clock").Inc()
		return
	}
	c.metrics.uncertainty.Observe(float64(uncertainty) / 1000)
	if uncertainty > 50 {
		c.metrics.probes.WithLabelValues("uncertain_clock").Inc()
		return
	}
	// 单程到达必定处于这次往返之内；拒绝超出时钟误差界的样本，避免坏时钟污染分位数。
	if *receipt.Received < receipt.Sent-uncertainty || *receipt.Received > receipt.Sent+rtt.Milliseconds()+uncertainty+1 {
		c.metrics.probes.WithLabelValues("invalid_clock").Inc()
		return
	}
	delay := max(int64(0), *receipt.Received-receipt.Sent)
	c.metrics.received.Observe(float64(delay) / 1000)
	c.metrics.probes.WithLabelValues("calibrated").Inc()
}

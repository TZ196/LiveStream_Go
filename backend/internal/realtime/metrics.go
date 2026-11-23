package realtime

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 每个 API 实例有自己的注册表。标签只用固定结果，避免按用户、连接或场次创建时间序列。
type deliveryMetrics struct {
	registry         *prometheus.Registry
	projection       *prometheus.HistogramVec
	projectionErrors *prometheus.CounterVec
	memory           prometheus.Histogram
	memoryResults    *prometheus.CounterVec
	fanout           prometheus.Histogram
	fanoutResults    *prometheus.CounterVec
	flush            prometheus.Histogram
	skips            *prometheus.CounterVec
	depth            prometheus.Histogram
	closes           *prometheus.CounterVec
	written          prometheus.Histogram
	received         prometheus.Histogram
	roundTrip        prometheus.Histogram
	uncertainty      prometheus.Histogram
	probes           *prometheus.CounterVec
}

func newDeliveryMetrics(m *Manager) *deliveryMetrics {
	r := prometheus.NewRegistry()
	f := promauto.With(r)
	buckets := []float64{.000001, .00001, .0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .15, .19, .2, .225, .25, .4, .5, 1, 2, 5, 10, 30, 60}
	hist := func(name, help string) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{Namespace: "livestream", Name: name, Help: help, Buckets: buckets}
	}
	results := func(name, help, label string) *prometheus.CounterVec {
		return f.NewCounterVec(prometheus.CounterOpts{Namespace: "livestream", Name: name, Help: help}, []string{label})
	}
	v := &deliveryMetrics{
		registry:         r,
		projection:       f.NewHistogramVec(hist("like_produce_to_redis_seconds", "Server Kafka CreateTime to successful Redis projection."), []string{"result"}),
		projectionErrors: results("like_projection_exclusions_total", "Projection failures or records without a usable producer clock.", "reason"),
		memory:           f.NewHistogram(hist("like_redis_to_snapshot_seconds", "Redis return to changed audience state, including lock wait.")),
		memoryResults:    results("like_snapshot_updates_total", "Outcome of synchronizing audience state after Redis.", "result"),
		fanout:           f.NewHistogram(hist("like_snapshot_to_enqueue_seconds", "First pending room update to last successful enqueue in its fanout.")),
		fanoutResults:    results("like_fanouts_total", "Room snapshot fanouts by recipient coverage.", "result"),
		flush:            f.NewHistogram(hist("like_flush_seconds", "Wall time of a complete room scan and fanout.")),
		skips:            results("broadcast_skips_total", "Broadcast attempts deferred by lock contention or failed board reads.", "reason"),
		depth:            f.NewHistogram(prometheus.HistogramOpts{Namespace: "livestream", Name: "ws_queue_depth", Help: "Queue occupancy observed at enqueue attempts on sampled connections.", Buckets: []float64{0, 1, 4, 16, 32, 64, 96, 115, 127, 128}}),
		closes:           results("ws_closes_total", "Connections by the first effective close reason.", "reason"),
		written:          f.NewHistogram(hist("ws_enqueue_to_write_seconds", "Sampled LIKE_UPDATE enqueue to successful WebSocket write completion.")),
		received:         f.NewHistogram(hist("ws_browser_receive_seconds", "Calibrated sampled LIKE_UPDATE write start to browser message callback.")),
		roundTrip:        f.NewHistogram(hist("ws_receipt_roundtrip_seconds", "Server write start to matching browser receipt on the server monotonic clock.")),
		uncertainty:      f.NewHistogram(hist("ws_clock_uncertainty_seconds", "Estimated clock uncertainty reported with browser samples.")),
		probes:           results("ws_delivery_probes_total", "Sampled writes, receipts and discarded timing estimates.", "result"),
	}
	for _, kind := range []string{"likes", "board"} {
		var lastLog atomic.Int64
		f.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "livestream", Name: "oldest_pending_seconds", Help: "Age of the oldest outstanding room update at scrape time.", ConstLabels: prometheus.Labels{"kind": kind}}, func() float64 {
			var oldest *time.Time
			var session, room uint64
			for id, view := range m.roomList() {
				if view.closed.Load() {
					continue
				}
				pending := view.boardSince.Load()
				if kind == "likes" {
					pending = view.pendingSince.Load()
					sending := view.sendingSince.Load()
					if sending != nil && (pending == nil || sending.Before(*pending)) {
						pending = sending
					}
				}
				if pending != nil && (oldest == nil || pending.Before(*oldest)) {
					oldest, session, room = pending, id, view.roomID
				}
			}
			if oldest == nil {
				return 0
			}
			now := time.Now()
			age := now.Sub(*oldest)
			threshold := 400 * time.Millisecond
			if kind == "board" {
				threshold = 2 * time.Second
			}
			last := lastLog.Load()
			if age > threshold && now.Unix()-last >= 30 && lastLog.CompareAndSwap(last, now.Unix()) {
				slog.Warn("oldest_realtime_pending", "kind", kind, "room_id", room, "session_id", session, "age", age)
			}
			return age.Seconds()
		})
	}
	return v
}

// MetricsHandler 输出本实例的指标。抓取待发年龄只读时间戳，即使场次锁忙也能观察积压。
func (m *Manager) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(m.metrics.registry, promhttp.HandlerOpts{})
}

// ObserveProjection 使用 Kafka 记录的生产者时间。业务事件中的 UUIDv7 时间来自浏览器。
func (m *Manager) ObserveProjection(record *kgo.Record, duplicate bool, err error, at time.Time) {
	if err != nil {
		m.metrics.projectionErrors.WithLabelValues("redis_error").Inc()
		return
	}
	if record.Attrs.TimestampType() != 0 || record.Timestamp.IsZero() || at.Before(record.Timestamp) {
		m.metrics.projectionErrors.WithLabelValues("producer_clock").Inc()
		return
	}
	result := "applied"
	if duplicate {
		result = "duplicate"
	}
	m.metrics.projection.WithLabelValues(result).Observe(at.Sub(record.Timestamp).Seconds())
}

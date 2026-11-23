package realtime

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// 公共帧只编码一次。采样连接发送总数时才复制这个信封并记录本连接的入队时刻。
type outbound struct {
	payload []byte
	total   *likeUpdate
	queued  time.Time
}

func encodeFrame(kind string, data any) ([]byte, error) {
	return json.Marshal(struct {
		Version int    `json:"version"`
		Type    string `json:"type"`
		Data    any    `json:"data"`
	}{1, kind, data})
}

type connection struct {
	socket   *websocket.Conn
	send     chan *outbound
	done     chan struct{}
	delivery sync.Mutex
	physical sync.Once
	metrics  *deliveryMetrics
	probe    *deliveryProbe
}

func (m *Manager) newConnection(socket *websocket.Conn) *connection {
	c := &connection{socket: socket, send: make(chan *outbound, 128), done: make(chan struct{}), metrics: m.metrics}
	if (m.connections.Add(1)-1)%64 == 0 {
		c.probe = &deliveryProbe{}
	}
	return c
}

// 调用方持有 delivery。首次关闭立即禁止任何新入队，物理关闭交给连接的读写协程。
func (c *connection) stopLocked(reason string) {
	select {
	case <-c.done:
	default:
		close(c.done)
		c.metrics.closes.WithLabelValues(reason).Inc()
	}
}

func (c *connection) close(reason string) {
	c.delivery.Lock()
	c.stopLocked(reason)
	c.delivery.Unlock()
	c.physical.Do(func() { _ = c.socket.Close() })
}

// offer 在持有 delivery 时执行；队列满只唤醒已有写协程，不等待 socket.Close。
func (c *connection) offer(msg *outbound) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	if c.probe != nil {
		c.metrics.depth.Observe(float64(len(c.send)))
		if msg.total != nil {
			copy := *msg
			copy.queued = time.Now()
			msg = &copy
		}
	}
	select {
	case c.send <- msg:
		return true
	default:
		c.stopLocked("queue_full")
		return false
	}
}

func (c *connection) enqueue(payload []byte) {
	c.delivery.Lock()
	c.offer(&outbound{payload: payload})
	c.delivery.Unlock()
}

// 每个收件人在自己的投递锁内核对场次和可用性。慢连接与关房都不计为投递成功。
func (m *Manager) sendRoomFrame(view *audience, clients []*connection, msg *outbound, available bool) (int, time.Time) {
	var last time.Time
	count := 0
	for _, c := range clients {
		c.delivery.Lock()
		if !view.closed.Load() && m.available.Load() == available && c.offer(msg) {
			count++
			if msg.total != nil {
				last = time.Now()
			}
		}
		c.delivery.Unlock()
	}
	return count, last
}

// writeLoop 独占写入。已有写入由 5 秒期限约束，退出时物理关闭唤醒读协程。
func (c *connection) writeLoop(expires time.Time, initial []byte) {
	reason := "peer_closed"
	defer func() {
		c.close(reason)
		if c.probe != nil {
			c.expireProbe(time.Now(), true)
		}
	}()
	select {
	case <-c.done:
		return
	default:
	}
	write := func(kind int, data []byte) bool {
		_ = c.socket.SetWriteDeadline(time.Now().Add(5 * time.Second))
		err := c.socket.WriteMessage(kind, data)
		if err == nil {
			return true
		}
		reason = "write_error"
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			reason = "write_timeout"
		}
		return false
	}
	if !write(websocket.TextMessage, initial) {
		return
	}
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	expiry := time.NewTimer(time.Until(expires))
	defer expiry.Stop()
	for {
		select {
		case <-c.done:
			return
		default:
		}
		select {
		case msg := <-c.send:
			data := msg.payload
			if !msg.queued.IsZero() {
				data = c.stampSample(msg)
			}
			if !write(websocket.TextMessage, data) {
				return
			}
			if !msg.queued.IsZero() {
				c.metrics.written.Observe(time.Since(msg.queued).Seconds())
			}
		case <-ping.C:
			if c.probe != nil {
				c.expireProbe(time.Now(), false)
			}
			if !write(websocket.PingMessage, nil) {
				return
			}
		case <-expiry.C:
			reason = "auth_expired"
			return
		case <-c.done:
			return
		}
	}
}

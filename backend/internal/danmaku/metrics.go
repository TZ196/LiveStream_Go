package danmaku

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Registry       *prometheus.Registry
	Review         prometheus.Histogram
	Limit          prometheus.Histogram
	Publish        prometheus.Histogram
	Submit         prometheus.Histogram
	Enqueue        prometheus.Histogram
	ReceiveEnqueue prometheus.Histogram
	Persist        prometheus.Histogram
	Results        *prometheus.CounterVec
	Consumer       *prometheus.GaugeVec
	Lag            *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	metrics := &Metrics{Registry: prometheus.NewRegistry()}
	// 桶仅为观测配置，不代表性能承诺；每项分别保留原始观测值。
	histogram := func(name, help string) prometheus.Histogram {
		value := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "livestream_danmaku_" + name + "_seconds", Help: help, Buckets: []float64{.00001, .0001, .001, .005, .01, .025, .05, .1, .25, .5, 1, 3, 10}})
		metrics.Registry.MustRegister(value)
		return value
	}
	metrics.Review = histogram("review", "AC 审核副本扫描耗时")
	metrics.Limit = histogram("limit", "Redis 原子限流及请求登记耗时")
	metrics.Publish = histogram("publish", "发布调用至 Broker 确认或未确认返回的耗时")
	metrics.Submit = histogram("submit", "服务端弹幕提交处理耗时")
	metrics.Enqueue = histogram("enqueue", "Kafka 生产时间至连接发送队列入队结束耗时，不是浏览器接收延迟")
	metrics.ReceiveEnqueue = histogram("receive_enqueue", "本次 API 接收至连接发送队列入队结束耗时，重放单列评估")
	metrics.Persist = histogram("persist", "Kafka 生产时间至 MySQL 事务提交耗时")
	metrics.Results = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "livestream_danmaku_requests_total", Help: "按提交结果计数"}, []string{"result"})
	metrics.Consumer = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "livestream_danmaku_consumer_ready", Help: "各弹幕消费组运行状态"}, []string{"group"})
	metrics.Lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "livestream_danmaku_partition_lag", Help: "各弹幕消费组逐分区积压"}, []string{"group", "partition"})
	metrics.Registry.MustRegister(metrics.Results, metrics.Consumer, metrics.Lag)
	return metrics
}

# Kafka 点赞 Producer 基础验证

2026-11-22，在 Windows 本地、Go 1.26.8 环境执行。仅验证独立事件构造与发送，尚未接入 API 和消费者。

- `go mod tidy`、`go mod verify`、`go vet ./...`、`go test ./...`、`go build ./cmd/api ./cmd/migrate`：通过。其他尚无用例的包显示 no test files。
- `go test -v ./internal/messaging`：身份、时间窗口、重复请求稳定性通过。使用独立临时 Apache Kafka 4.0.0 单 Broker，创建 16 分区 `live.events`，同一事件发送两次，真实读到两条键和值一致的记录；测试后临时容器已停止。
- `go test -race ./internal/messaging`：未通过，当前环境 CGO 未启用，未执行竞争检测。
- Redis 原子去重、MySQL 唯一约束与事务、offset 故障、Kafka 确认丢失、端到端点击和 k6 压测：尚未实现或未验证；本结果不能证明端到端幂等或无损。

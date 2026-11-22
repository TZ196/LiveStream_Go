# 批量点赞、快照与消费组验收

2026-11-22，在 WSL2 Ubuntu 26.04.1 的独立 Compose 项目执行。版本为 Go 1.26.8、Node 26.13.0、Chromium 159、MySQL 8.4.13、Redis 7.4.7、Kafka 4.0.0。一个 API 和一个 Worker 使用 race 构建，Kafka 为单 Broker，live.events 为 16 分区、单副本。测试使用独立端口、账号、场次、Topic 和数据卷。

| 场景 | 操作及实际结果 |
| --- | --- |
| COUNT-01、IDEM-03，连点和 ACK 丢失 | 两位用户在同一房间，一位观众在另一房间。最终构建中浏览器首击后 501 ms 发出一个 count=5。屏蔽发送方收到的第一条 ACK，再点 7 次，依次观察到 5、7、5 三个发送数量。第一批在 3001 ms 后重用完整原帧。MySQL 两行共 12 赞。 |
| 多人点赞 | 两位用户各连点 31、37 次。两个页面都从 12 更新至 80，MySQL 四行共 80 赞。 |
| 合并广播与 Worker 追平 | 停止 Worker，用已认证的 WebSocket 连发 24 个不同 ID，每批 100 赞。三次完整浏览器运行中，同场观察者分别收到 3、2、2 份总数快照，另一房间收到 0 份。两位同场用户最终均显示 2480。停机时 MySQL 仍为四行共 80 赞，Worker 恢复后为 28 行共 2480 赞。 |
| KAFKA-01，组内分配与重启 | Kafka 报告实时与持久化组各 4 人，各组内每人 4 个分区。仅调整配置并重启后，实时组 3 个成员分别持有 5、5、6 个分区，持久化组 2 个成员各持有 8 个。API 重启后浏览器从 ROOM_STATE 恢复到 2480，与 Redis 一致。 |
| ROOM-05，重新开播 | 相同房间新开一场并点 3 次，新场 MySQL 总数为 3，旧场保持 2480。 |
| ENV-03，旧数据迁移 | 在版本 4 的临时库插入一行旧点赞，迁移到 5 后原行的 click_count 为 1，重复执行迁移仍为一行一个赞。另从空库依次执行到版本 5。 |
| 协议与故障边界 | 自动化覆盖同 ID 改数量、重复消费、旧版空载荷、零/负/小数及整数范围校验、计数溢出时保留原投影和检查点。有效的 100000 赞批次在 Redis 重放后仍为 100000。不同分区乱序返回时，客户端只收到较高版本的快照。 |

Go 格式、vet、三个入口构建及前端类型检查、构建均通过。真实中间件上的 Go race 测试执行 11 个用例，全部通过。前端时钟回归执行 1 个用例，通过。

## 复核方法

前端回归使用真实 Vue 状态模块、可控时钟和模拟传输，覆盖第 499/500 毫秒、下一批与旧 ACK 独立、同场重连、旧版队列与新场隔离。[前端用例](../../../frontend/tests/likes.test.mjs)

```sh
cd frontend
npm ci
npm run type-check
npm test
npm run build
```

后端测试使用全新的隔离 MySQL、Redis 和 Kafka，并按部署说明创建 live.events。加载对应开发配置，并将 LIVESTREAM_TEST_MYSQL_DSN、LIVESTREAM_TEST_REDIS 和 LIVESTREAM_TEST_KAFKA 指向这些测试服务，再执行下列命令。双消费组用例把旧单击及其升级后重试、一个 37 赞批次及其重放发给两组，核对 Redis 与数据库两行共 38 赞。[双消费组用例](../../../backend/internal/messaging/dual_consumer_integration_test.go)

```sh
cd backend
go run ./cmd/migrate
go vet ./...
go test ./...
go test -race -v -count=1 -timeout 120s ./...
go build ./cmd/api ./cmd/migrate ./cmd/worker
```

浏览器验证通过页面按钮点击、监听 WebSocket 帧、按 request_id 对比重试，及按 session_id 查询数据库完成。API 的 ACK、观察者收到的 LIKE_UPDATE 和数据库落盘分别计数。可用以下查询对照同场最终总数，以及通过 Kafka 命令检查当前的成员分配。[部署说明](../../deployment.md#本地点赞链路启动powershell)

```sql
SELECT session_id, COUNT(*) AS batches, SUM(click_count) AS clicks
FROM interaction_events
GROUP BY session_id;
```

浏览器只屏蔽第一条下行 ACK，Broker 和两个消费组实际运行。200 ms 是服务端推送时钟，消息数取决于事件抵达各个窗口的时间。上述负载用于验证计数、重试和隔离。持续吞吐、尾延迟、Kafka 集群容灾及 Redis 全量重建仍待相应阶段验收。覆盖参考审查的 REF-02、ISSUE-03、ISSUE-04、ISSUE-08。[参考审查](../../reviews/live-stream-danmaku-review.md)

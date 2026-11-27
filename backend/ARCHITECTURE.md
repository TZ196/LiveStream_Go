# 从源码理解后端

这份导航帮助你从一次用户操作走到代码实现。它描述当前代码的位置和调用顺序；协议细节只在 [docs 设计文档](../docs/README.md) 中维护。当前是一个 Go Module、一个 API 进程和一个 Worker 进程；不是多实例部署。

## 先认识入口和分层

1. [cmd/api/main.go](cmd/api/main.go)：加载配置，创建数据库、服务和共享连接管理器，启动 HTTP 服务，并安排退出。
2. [internal/httpserver/server.go](internal/httpserver/server.go)：创建 Hertz，注册路由和中间件。Handler 校验输入、调用业务并映射响应。
3. `internal/*/service.go`：定义业务规则。Repository 负责 GORM 查询和事务，不把 Hertz 请求上下文传入业务层。
4. [cmd/worker/main.go](cmd/worker/main.go)：分别运行点赞与弹幕持久化消费者；一个业务失败不自动取消另一个。
5. [cmd/migrate/main.go](cmd/migrate/main.go) 与 [migrations/migrate.go](migrations/migrate.go)：独立执行版本迁移；API/Worker 启动只检查结构，不自动迁移。

实际所需数据库版本由 [database/mysql.go](internal/database/mysql.go) 的 `RequiredSchemaVersion` 决定；当前为 V9。执行迁移前先读 [迁移边界](../docs/design/database.md#迁移与版本)，不要把编写迁移等同于授权执行生产迁移。

## 账号与房间

注册入口是 [httpserver/auth.go](internal/httpserver/auth.go) 的 `register`，再到 [user/service.go](internal/user/service.go) 校验用户名和密码，[user/password.go](internal/user/password.go) 哈希密码，最后由 [user/repository.go](internal/user/repository.go) 写 MySQL。登录验证成功后由 [user/token.go](internal/user/token.go) 签发 JWT；后续身份由认证中间件解析。

房间入口在 [httpserver/room.go](internal/httpserver/room.go)。[room/service.go](internal/room/service.go) 处理权限与状态，[room/repository.go](internal/room/repository.go) 负责事务：创建房间时建立首个场次，重新开播时建立新场次。为什么要区分长期房间与单次场次，见 [领域模型](../docs/design/domain-model.md#房间状态)。

## 连接：先取得票据，再升级 WebSocket

浏览器持 Bearer 调用 [httpserver/realtime.go](internal/httpserver/realtime.go) 的票据接口；[realtime/ticket.go](internal/realtime/ticket.go) 签发、核销绑定房间、场次和 Origin 的一次性票据。Hertz 将 `/ws/rooms/:id` 经 adaptor 交给 [realtime/manager.go](internal/realtime/manager.go)，后者检查票据并开始读循环。

连接放在 [realtime/audience.go](internal/realtime/audience.go) 的场次对象中，[delivery.go](internal/realtime/delivery.go) 管理单连接写队列。[ownership.go](internal/realtime/ownership.go) 用 Redis 确认账号归属，[presence.go](internal/realtime/presence.go) 替换本地连接并生成在线人数。握手、消息和配额见 [WebSocket 协议](../docs/design/websocket-protocol.md)。

## 点赞：一条请求，两个独立消费组

```text
浏览器批量点击 → WS LIKE → Kafka live.events
                         ├─ live-realtime → Redis 投影 → 本机连接推送
                         └─ live-persistence → MySQL 事务
```

1. [frontend/src/likes.ts](../frontend/src/likes.ts) 聚合 500 毫秒内点击，保留待确认批次；[realtime/likes.go](internal/realtime/likes.go) 校验消息并交给 Producer。
2. [likes/event.go](internal/likes/event.go) 构造稳定事件身份；[likes/kafka_producer.go](internal/likes/kafka_producer.go) 等待 Broker 确认后返回。ACK 只代表 Kafka 接受，不代表广播送达或 MySQL 已落库。
3. [cmd/api/likes.go](cmd/api/likes.go) 的 `processLike` 调用 [likes/projection.go](internal/likes/projection.go) 与 [project_like.lua](internal/likes/lua/project_like.lua)，原子去重并更新 Redis 投影。随后 [realtime/likes_updates.go](internal/realtime/likes_updates.go) 和 [realtime/likes_leaderboard.go](internal/realtime/likes_leaderboard.go) 合并并推送状态。
4. Worker 使用 [likes/persistence.go](internal/likes/persistence.go) 在同一事务中写 `consumer_inbox`、点赞事件和检查点；成功后才提交本消费组的 Kafka 位移。两组各自接收事件，组内成员是分工，不是多 API 实例广播。

需要理解重复消息、结果未知或 Redis 重建时，再读 [可靠性设计](../docs/design/reliability.md)。

## 弹幕：审核后发布，三条独立消费路径

```text
浏览器 WS DANMAKU → 登记/限流 → AC 审核 → Kafka live.danmaku
                                           ├─ danmaku-realtime → 场次分发 → 本机连接
                                           ├─ danmaku-persistence → MySQL
                                           └─ danmaku-recent-cache → Redis 最近缓存
```

1. [realtime/danmaku.go](internal/realtime/danmaku.go) 接收消息，[danmaku/service.go](internal/danmaku/service.go) 按顺序核对原请求、登记限流、审核、冻结事件并发布。审核器由 [danmaku/moderation.go](internal/danmaku/moderation.go) 在启动时加载 [词库](configs/moderation_words.tsv)；命中时不发布 Kafka。
2. [cmd/api/danmaku.go](cmd/api/danmaku.go) 启动实时组和最近缓存组。[realtime/danmaku_delivery.go](internal/realtime/danmaku_delivery.go) 使用每场有界队列串行分发；[danmaku/recent.go](internal/danmaku/recent.go) 维护每场 Redis ZSET，最近查询每页最多 20 条。
3. [cmd/worker/main.go](cmd/worker/main.go) 调用 [danmaku/persistence.go](internal/danmaku/persistence.go)，将弹幕及检查点放在同一事务；弹幕依靠业务表唯一键去重，不使用点赞的 `consumer_inbox`。[failed_events.go](internal/danmaku/failed_events.go) 处理可隔离的永久异常。
4. [httpserver/danmaku.go](internal/httpserver/danmaku.go) 暴露最近缓存与既有 MySQL 历史接口。[frontend/src/danmaku.ts](../frontend/src/danmaku.ts) 在连接后请求最近缓存；[RoomDanmaku.vue](../frontend/src/components/RoomDanmaku.vue) 处理展示和上拉分页。缓存缺失时 [recent_recovery.go](internal/danmaku/recent_recovery.go) 等待持久化屏障，再从现有 MySQL 弹幕表重建；恢复刚完成时屏障后的消费仍可能有延迟。

V2 事件冻结服务端接收时间，供最近缓存及重建使用；V1 仍可读取，但没有可靠的服务端时间，不进入 V2 最近缓存。详细时间、分页及故障边界见 [可靠性设计](../docs/design/reliability.md#最近弹幕旁路缓存)。

## 遇到故障时从哪里看

- 发布结果未知：看 [danmaku/service.go](internal/danmaku/service.go) 的 `Submit` 和 [可靠性设计](../docs/design/reliability.md#弹幕登记审核与结果核对)；重试不能随意更换请求 ID。
- 点赞 Redis 丢失：看 [likes/recovery.go](internal/likes/recovery.go)，它在检查依据和 Worker 进度后重建投影；不是简单清零继续消费。
- Kafka 或 MySQL 故障：看 [likes/kafka_consumer.go](internal/likes/kafka_consumer.go)、[danmaku/kafka_consumer.go](internal/danmaku/kafka_consumer.go) 和两条持久化路径。业务事务或可靠隔离成功后才推进相应组位移。
- 运行与验证：从 [运行说明](../docs/deployment.md) 和 [项目状态](../docs/status.md) 找实际命令与历史报告。`*_test.go` 与 `frontend/tests/` 按当前仓库规则仅保留本地；新检出的 `go test` 若显示 `no test files`，不代表执行了用例。

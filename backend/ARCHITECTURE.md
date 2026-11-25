# Backend 结构

## 目录职责

| 路径 | 打开这里可以看到什么 |
| --- | --- |
| cmd/api/main.go、likes.go、danmaku.go | main 拥有共享连接生命周期；likes 装配原点赞与恢复，danmaku 装配审核与独立消费 |
| cmd/worker | 一个进程内两条独立的点赞/弹幕持久化循环，单业务失败不取消另一条 |
| cmd/migrate | 数据库版本迁移命令 |
| cmd/backfill-like-board | 维护窗口内为已有场次补建榜单 |
| internal/httpserver | Hertz 路由、参数校验、认证和响应，middleware.go 包含访问日志与登录限流 |
| internal/user | 注册登录、密码、用户查询及 token.go 中的 JWT 身份 |
| internal/room | 房间权限、开关播、独立场次与数据库事务 |
| internal/likes | event.go 的事件规则，kafka_*.go 的收发和消费组，projection.go 的 Redis 总数、个人数和榜单，persistence.go 的 MySQL 事务，initializer.go 的初始化与检查，recovery.go 的一次性检查/重建，backfill.go 的补榜 |
| internal/realtime | manager.go 共享握手与读循环，capabilities.go 能力快照，audience.go 场次与成员，delivery.go 唯一写循环，ticket.go 票据；业务与监控归类见下表 |
| internal/danmaku | event → service（准入）→ requests 与 lua/（Redis 状态/房间限流）→ moderation（AC）→ kafka_producer；kafka_consumer 解码、短重试与提交，persistence 事务与历史，failed_events 异常仓储，metrics 指标 |
| internal/config、database、logging | 配置读取、GORM 连接池及结构检查、应用与框架日志 |
| configs、migrations | 无秘密的配置样例，各版本数据库迁移及调度 |

日常启动入口为 `go run ./cmd/api` 和 `go run ./cmd/worker`，先按[运行说明](../docs/deployment.md)配置环境和执行迁移。`bin/` 是可重新构建的本地输出目录。弹幕新增迁移 7、8；本次只追加 V8 异常表，迁移 1–7 不改写，原点赞恢复仍集中在 likes。

`realtime` 保持一个 Go 包，以文件职责分类，共用同一套连接和锁，不额外拆出互相依赖的子包：

| 分类 | 文件与职责 |
| --- | --- |
| 公共连接 | manager.go 调度握手、读帧和生命周期；audience.go 管成员；delivery.go 管有界入队及唯一写循环；ticket.go 管票据 |
| 公共能力 | capabilities.go 生成 ROOM_STATE、刷新两个业务能力 |
| 弹幕 | danmaku.go 接入、响应映射和状态；danmaku_delivery.go 按场次串行分发 |
| 点赞 | likes.go 接入、准入及恢复快照；likes_updates.go 总数推送；likes_leaderboard.go 本人与榜单推送 |
| 监控 | metrics.go 指标；delivery_probe.go 采样投递观测 |

## 模块依赖

```text
cmd/api → httpserver → user、room、realtime
cmd/api → likes、danmaku、realtime、database、config、logging
cmd/worker → likes、danmaku、database、config、logging
cmd/migrate → migrations → database
cmd/backfill-like-board → likes、database、config
realtime → likes、danmaku、room、user
likes → Kafka、Redis、GORM/MySQL
danmaku → Kafka、Redis、GORM/MySQL、room、user
user → GORM/MySQL、密码哈希、JWT
room → GORM/MySQL
database → config、GORM/MySQL
```

箭头列出主要直接依赖，外部能力按用途概括。cmd 创建具体实现，httpserver 接收请求，业务包维护规则和仓储，realtime 维护连接与场次。新增账号、房间、点赞功能时扩展对应包。本次保留点赞 Kafka 代码，在 danmaku 内实现其独立收发，避免为两个恢复边界不同的业务新增通用 messaging 框架。详细职责和并发约束见[系统架构](../docs/design/architecture.md)。

## 点赞完整业务链路

1. httpserver 验证 Bearer，签发绑定用户、房间、场次和来源的一次性票据。realtime.Manager 完成握手和注册，连接写循环先发送 Redis 权威首屏。
2. readMessages 解析 LIKE，校验场次并调用 likes.NewLikeBatch 构造稳定事件。Producer 获得 Broker 确认后，连接才收到接受 ACK。
3. API 内的实时组读取 live.events。Consumer 对记录解码校验一次，processLike 用同一事件初始化场次投影、调用 Redis Lua，并按版本同步在线场次的总数、个人数和榜单待发状态，然后消费循环提交本组位移。
4. 总数与个人、榜单的周期分发分别在 likes_updates.go 和 likes_leaderboard.go。delivery.go 向各连接有界入队，由每连接唯一写循环发送。个人更新只送该用户在本场的连接。
5. Worker 的独立持久化组读取同一日志，经相同校验后，在一笔 MySQL 事务内完成 inbox 去重、业务事件和检查点。事务成功后消费循环才提交该组位移。

请求身份、JSON、分区键和接受规则分别由[可靠性设计](../docs/design/reliability.md)、[Kafka 契约](../docs/design/kafka-events.md)和[WebSocket 契约](../docs/design/websocket-protocol.md)维护。存储键、索引与迁移规则见[数据设计](../docs/design/database.md)。

## 弹幕阅读与运行链路

先读 internal/danmaku/service.go，可以顺着看到：核对原请求 → 新请求原子登记和限流 → AC 审核 → 冻结原文及审核版本 → Kafka 确认。词库位于 configs/moderation_words.tsv，人工词库修改后重启 API 生效。

Kafka 的 live.danmaku 有两个独立组：API 中的 danmaku-realtime 经场次串行队列向连接入队；Worker 中的 danmaku-persistence 在一笔事务保存消息及检查点。历史 HTTP 直接调用弹幕 Repository，不再经过 Kafka。前端 realtime.ts 只拥有一条连接，likes.ts 和 danmaku.ts 分别维护各自的待确认队列。

点赞恢复只暂停点赞，保留共享连接；新的 ROOM_STATE 更新能力和投影代次。弹幕词库、消费者或持久化循环失败只暂停相应路径；Redis 整体不可用则两个依赖 Redis 的业务都可能受影响。验收与限制见[弹幕报告](../docs/verification/reports/danmaku.md)。

## Kafka、Redis、MySQL 故障恢复边界

Kafka 发布超时保留结果未确认，客户端重试原始请求。弹幕永久坏消息有限重试后，通过 failed_events 可靠隔离再继续；隔离失败或依赖故障仍停止本组，核对 Topic 身份和存储检查点后重建。点赞继续沿用原恢复流程。两个业务的消费组独立运行。

Redis 已更新而内存等锁超时，会保留未提交的 Kafka 位置。重放通过事件标记取得当前计数，再补齐在线快照。正常重启逐分区检查计数、榜单和去重依据，保留完整数据。身份、场次投影或进度缺失时停止后端接收，Worker 追平后按[恢复设计](../docs/design/reliability.md#redis-丢失后的恢复步骤)在新代次重建。前端持续采集并暂存，取得同场首屏后补发。backfill-like-board 处理的是已有投影的历史榜单补建。

MySQL 事务失败时 inbox、事件和检查点一起回滚，重试依靠原事件身份去重。弹幕 Worker 的异常记录也与检查点同事务，事务已成功而 Kafka 提交失败时可重新读取。恢复不建立任务表或断点续建；点赞状态独立于普通 API 就绪。点赞异常隔离及关房在途完成屏障的进度见[项目状态](../docs/status.md)，实际验证见[弹幕报告](../docs/verification/reports/danmaku.md)。

## 本地验证文件

按用户要求，`*_test.go`、frontend/tests/ 和测试产物仅保留本地并被 Git 忽略。新检出仓库可构建程序，但没有这些本地测试用例；`go test` 输出 no test files 不表示执行了测试。现有报告中的历史测试命令依赖当时的本地源码和隔离环境，不是远程可直接运行的交付物。历史 HTTP 的限流、并发和缓存保护按用户要求暂缓，查询路径保持原样。

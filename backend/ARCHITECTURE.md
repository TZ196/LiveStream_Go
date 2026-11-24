# Backend 结构

## 目录职责

| 路径 | 打开这里可以看到什么 |
| --- | --- |
| cmd/api/main.go、likes.go | HTTP 启动与退出，实时点赞依赖装配、逐条处理和消费组恢复 |
| cmd/worker | 独立的点赞持久化进程 |
| cmd/migrate | 数据库版本迁移命令 |
| cmd/backfill-like-board | 维护窗口内为已有场次补建榜单 |
| internal/httpserver | Hertz 路由、参数校验、认证和响应，middleware.go 包含访问日志与登录限流 |
| internal/user | 注册登录、密码、用户查询及 token.go 中的 JWT 身份 |
| internal/room | 房间权限、开关播、独立场次与数据库事务 |
| internal/likes | event.go 的事件规则，kafka_*.go 的收发和消费组，projection.go 的 Redis 总数、个人数和榜单，persistence.go 的 MySQL 事务，initializer.go 的初始化与检查，recovery.go 的一次性检查/重建，backfill.go 的补榜 |
| internal/realtime | manager.go 的握手和读循环，audience.go 的场次状态与锁，delivery.go 的队列和写循环，like_updates.go 与 leaderboard.go 的分发，票据与送达指标 |
| internal/config、database、logging | 配置读取、GORM 连接池及结构检查、应用与框架日志 |
| configs、migrations | 无秘密的配置样例，各版本数据库迁移及调度 |

日常启动入口为 `go run ./cmd/api` 和 `go run ./cmd/worker`，先按[运行说明](../docs/deployment.md)配置环境和执行迁移。`bin/` 是可重新构建的本地输出目录。内部包保持 8 个；恢复实现集中在 likes，新增 Migration 只保存有效投影代次。

## 模块依赖

```text
cmd/api → httpserver → user、room、realtime
cmd/api → likes、realtime、database、config、logging
cmd/worker → likes、database、config、logging
cmd/migrate → migrations → database
cmd/backfill-like-board → likes、database、config
realtime → likes、room、user
likes → Kafka、Redis、GORM/MySQL
user → GORM/MySQL、密码哈希、JWT
room → GORM/MySQL
database → config、GORM/MySQL
```

箭头列出主要直接依赖，外部能力按用途概括。cmd 创建具体实现，httpserver 接收请求，业务包维护规则和仓储，realtime 维护连接与场次。新增账号、房间、点赞功能时扩展对应包。弹幕交付时建立其业务包，再依据实际复用提取 Kafka 收发和消费组实现。详细职责和并发约束见[系统架构](../docs/design/architecture.md)。

## 点赞完整业务链路

1. httpserver 验证 Bearer，签发绑定用户、房间、场次和来源的一次性票据。realtime.Manager 完成握手和注册，连接写循环先发送 Redis 权威首屏。
2. readMessages 解析 LIKE，校验场次并调用 likes.NewLikeBatch 构造稳定事件。Producer 获得 Broker 确认后，连接才收到接受 ACK。
3. API 内的实时组读取 live.events。Consumer 对记录解码校验一次，processLike 用同一事件初始化场次投影、调用 Redis Lua，并按版本同步在线场次的总数、个人数和榜单待发状态，然后消费循环提交本组位移。
4. 总数与个人、榜单的周期分发分别在 like_updates.go 和 leaderboard.go。delivery.go 向各连接有界入队，由每连接唯一写循环发送。个人更新只送该用户在本场的连接。
5. Worker 的独立持久化组读取同一日志，经相同校验后，在一笔 MySQL 事务内完成 inbox 去重、业务事件和检查点。事务成功后消费循环才提交该组位移。

请求身份、JSON、分区键和接受规则分别由[可靠性设计](../docs/design/reliability.md)、[Kafka 契约](../docs/design/kafka-events.md)和[WebSocket 契约](../docs/design/websocket-protocol.md)维护。存储键、索引与迁移规则见[数据设计](../docs/design/database.md)。

## Kafka、Redis、MySQL 故障恢复边界

Kafka 发布超时保留结果未确认，客户端重试原始请求。消费处理失败时本组停止拉取，回收在途成员，核对 Topic 身份和存储检查点后重建组。另一消费组独立运行。

Redis 已更新而内存等锁超时，会保留未提交的 Kafka 位置。重放通过事件标记取得当前计数，再补齐在线快照。正常重启逐分区检查计数、榜单和去重依据，保留完整数据。身份、场次投影或进度缺失时停止后端接收，Worker 追平后按[恢复设计](../docs/design/reliability.md#redis-丢失后的恢复步骤)在新代次重建。前端持续采集并暂存，取得同场首屏后补发。backfill-like-board 处理的是已有投影的历史榜单补建。

MySQL 事务失败时 inbox、事件和检查点一起回滚，重试依靠原事件身份去重。事务已成功而 Kafka 提交失败时可重新读取。恢复不建立任务表或断点续建；点赞状态独立于普通 API 就绪。当前异常事件持久隔离及关房在途完成屏障的进度见[项目状态](../docs/status.md)，实际验证见[本轮报告](../docs/verification/reports/backend-refactor.md)。

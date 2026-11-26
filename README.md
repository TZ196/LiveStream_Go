# LiveStream 实时直播互动系统

一个面向学习和本地开发的直播间互动项目：用户可以注册登录、管理直播间，在独立直播场次中发送弹幕和连续点赞。项目侧重实时连接、消息处理、数据持久化与故障恢复，不包含音视频推流。

## 已实现的功能

- **用户与房间**：注册、登录、JWT 认证和登录状态恢复；创建、查询、结束和重新开播房间，每次开播使用独立场次。
- **WebSocket 连接**：通过 HTTP 换取一次性房间票据，再建立共享连接；支持心跳、断线重连和慢连接保护。
- **弹幕与审核**：发送文本弹幕，使用 AC 自动机和本地词库审核；通过审核后写入 Kafka，再由实时消费者向同场观众广播。
- **点赞与榜单**：前端合并连续点击，展示本场点赞总数、前 100 名及当前用户的累计点赞数。
- **事件持久化与历史**：点赞和弹幕由独立 Worker 消费并写入 MySQL；直播页从 Redis 查询最近五分钟的弹幕，支持分页和缓存重建，另保留 MySQL 历史查询接口。
- **重试与恢复**：请求使用稳定标识，处理重复消费和结果未确认；点赞投影支持恢复，弹幕永久异常事件可隔离，具体边界见[可靠性设计](docs/design/reliability.md)。

## 技术栈

| 部分 | 技术 | 用途 |
| --- | --- | --- |
| 后端 | Go、CloudWeGo Hertz | HTTP API、中间件和业务服务 |
| 实时连接 | Gorilla WebSocket、Hertz HTTP Adaptor | WebSocket 握手和房间消息收发 |
| 数据访问 | GORM、MySQL 8.4 | 用户、房间、事件和消费检查点 |
| 实时数据 | Redis 7、Lua | 点赞投影、弹幕请求状态、限流和最近弹幕缓存 |
| 消息队列 | Apache Kafka | 缓冲事件，供实时、持久化和缓存消费者独立处理 |
| 前端 | Vue 3、TypeScript、Vite | 页面、状态管理和交互 |
| 本地环境 | Docker Compose | 启动 MySQL、Redis 和 Kafka 等中间件 |

依赖的具体版本以 [backend/go.mod](backend/go.mod)、[frontend/package.json](frontend/package.json) 和 [Compose 配置](deployments/docker/compose.yaml)为准。

## 核心链路

下面按功能展示数据流。API Server 持有 WebSocket 连接和实时消费者，Worker 负责异步持久化；消费组是各自维护位移的逻辑订阅者，不等同于独立部署的服务。

### 1. 注册、登录与房间管理

```text
注册 → HTTP API → 密码 Argon2id 哈希 → GORM → MySQL users
登录 → HTTP API → 查询 users、校验密码 → 签发 JWT → 浏览器
创建 / 查询 / 结束 / 重新开播 → JWT 鉴权 → 房间服务 → GORM → MySQL
                                                        └→ rooms + live_sessions
```

用户和房间是需要长期保存的业务数据，直接访问 MySQL；每次开播对应一个独立场次。这里没有为了使用中间件而经过 Kafka。房间状态与场次的修改由数据库事务协调，结束和重开仅房主可操作。

源码：[认证入口](backend/internal/httpserver/auth.go)、[用户服务](backend/internal/user/service.go)、[密码处理](backend/internal/user/password.go)、[房间服务](backend/internal/room/service.go)、[房间事务](backend/internal/room/repository.go)。

### 2. 进入直播间与维护连接

```text
浏览器携带 JWT → HTTP API 校验身份和场次 → 签发一次性票据（API 进程内存）
  → 携带票据发起 WebSocket 握手 → 校验票据、房间和 Origin → 注册到本机房间管理器
  → 读循环接收消息 / 写循环发送消息 → 心跳、断线重连或关闭时清理连接
```

票据有效期为 30 秒、只能消费一次；它保存在 API 进程内存，并非 Redis 共享票据。每条连接有独立写循环和有界发送队列，慢客户端不会无限阻塞房间广播。当前按单 API 实例设计，进程内连接和票据不能直接跨实例共享。

源码：[票据](backend/internal/realtime/ticket.go)、[连接管理与读循环](backend/internal/realtime/manager.go)、[写循环](backend/internal/realtime/delivery.go)、[前端连接与重连](frontend/src/realtime.ts)。

### 3. 点赞：批量发送、实时投影与持久化

```text
浏览器连续点击 → 500 ms 合并为一批，固定 request_id 和点击数
  → WebSocket → API 校验身份、场次和请求 → Kafka live.events
                                             ├→ Broker 确认 → API 返回接受结果
                                             ├→ live-realtime（API）
                                             │   → Redis Lua：事件去重 + 更新总数、个人数和榜单
                                             │   → API 合并更新 → WebSocket 推送房间观众
                                             └→ live-persistence（Worker）
                                                 → MySQL 事务：consumer_inbox 去重
                                                   + interaction_events + worker_checkpoints
                                                 → 成功后提交该消费组 Kafka 位移
```

前端合并点击以减少请求；Redis 保存用于实时展示的投影，榜单展示前 100 名。Lua 将去重和计数更新放在一次原子执行中，避免重复消费造成重复计数；Worker 则以 inbox 和数据库事务处理持久化去重。两条消费链路各自推进，因此 Redis 展示和 MySQL 落盘可能暂时不一致。

源码：[前端批处理](frontend/src/likes.ts)、[点赞接入](backend/internal/realtime/likes.go)、[Redis 投影](backend/internal/likes/projection.go)、[投影 Lua](backend/internal/likes/lua/project_like.lua)、[Worker 事务](backend/internal/likes/persistence.go)。

### 4. 弹幕：请求登记、审核、广播与保存

```text
浏览器输入弹幕 → WebSocket → API 身份和场次校验
  → Redis：登记请求状态、按用户和房间限流（10 秒最多 10 条新请求）
  → 本地词库 + AC 自动机审核
       ├→ 未通过：返回拒绝，不发布该弹幕
       └→ 通过：冻结事件 ID、审核版本和服务端接收时间 → Kafka live.danmaku
           ├→ Broker 确认 → API 返回接受结果
           ├→ danmaku-realtime（API）→ 房间管理器 → WebSocket 广播
           ├→ danmaku-persistence（Worker）→ MySQL 事务：danmaku_messages
           │                                  + worker_checkpoints → 提交 Kafka 位移
           └→ danmaku-recent-cache（API）→ Redis：对应房间和场次的最近弹幕 ZSET
```

服务端接收时间在首次成功冻结事件时确定，重试沿用原值；最近五分钟按此时间计算。弹幕使用消息表的唯一约束及事件内容校验去重，不写入点赞的 `consumer_inbox`。实时广播尽力送达，缓存和持久化各有独立位移，彼此可能延迟；实时组或持久化组遇到永久异常事件时可隔离，缓存组的异常处理见[事件设计](docs/design/kafka-events.md)。

源码：[弹幕接入](backend/internal/realtime/danmaku.go)、[请求及审核流程](backend/internal/danmaku/service.go)、[审核器](backend/internal/danmaku/moderation.go)、[持久化](backend/internal/danmaku/persistence.go)、[缓存消费者组装](backend/cmd/api/danmaku.go)。

### 5. 最近弹幕查询与缓存恢复

```text
进入直播间 / 向上翻页 → HTTP API → Redis 最近弹幕 ZSET
  → 按游标每次最多返回 20 条、只展示最近 5 分钟 → 浏览器

缓存缺失或未就绪 → 返回预热状态并触发后台恢复
  → Redis 恢复锁 → 记录 Kafka 分区末端 → 等待 Worker 的 MySQL 检查点追上
  → 从 MySQL 读取该场次最近 5 分钟 → 构建临时 ZSET → 原子切换为正式缓存
  → 浏览器重试查询；恢复期间该场次的缓存消费等待后重试
```

正常查询只读 Redis；MySQL 用于缓存重建和单独的历史查询接口。等待持久化检查点是为了避免从尚未落盘的数据重建出不完整缓存；恢复与继续消费共同衔接，但这不意味着读取、缓存和数据库始终同步。旧版没有可信服务端接收时间的弹幕不进入新版最近缓存。

源码：[最近查询](backend/internal/danmaku/recent.go)、[恢复流程](backend/internal/danmaku/recent_recovery.go)、[前端加载与翻页](frontend/src/danmaku.ts)；详细边界见[可靠性设计](docs/design/reliability.md)。

### 6. 如何理解跨中间件的一致性

- **请求接受不等于全部完成**：Kafka Broker 确认只说明发布已获得 Broker 确认，不代表观众已收到、Redis 已更新或 MySQL 已落盘；发布超时也不等于消息一定未写入。客户端需要保留原请求标识，按协议查询或重试。
- **避免直接同步双写**：点赞和弹幕先发布事件，再由不同消费组分别驱动实时处理与数据库持久化；弹幕发布前另有 Redis 请求登记，并非 Kafka 与 Redis 的跨系统事务。各步骤失败或超时时可能出现结果未知，系统依靠稳定身份、幂等处理与恢复机制处理，而不是宣称跨系统原子提交或端到端 Exactly Once。
- **数据库提交与位移提交是两步**：Worker 先在数据库事务中保存业务数据和检查点，再提交 Kafka 位移。若数据库已提交但位移提交失败，消息可能重放，去重约束防止重复产生业务效果；`consumer_inbox` 是点赞消费去重记录，并不是 Outbox 表。
- **可恢复也有边界**：Redis 投影是为实时读取准备的数据，可依照[可靠性设计](docs/design/reliability.md)恢复；离线用户无法保证收到每条 WebSocket 广播，单机 Kafka 与单 API 实例也不提供多机容灾。

## 本地运行

按照[开发与运行说明](docs/deployment.md)准备本地配置，使用 Compose 启动中间件和初始化 Topic，执行独立数据库迁移，再分别启动 API、Worker 和前端。当前代码要求数据库结构版本 V9；已有数据库升级前请先核对运行说明中的兼容和备份步骤。

前端默认地址为 http://127.0.0.1:5173，API 默认地址为 http://127.0.0.1:8080。每位协作者使用自己的本地配置与数据卷。

## 项目结构

```text
backend/
  cmd/api/          API 服务入口
  cmd/worker/       异步持久化入口
  cmd/migrate/      独立数据库迁移入口
  internal/         用户、房间、点赞、弹幕及实时连接实现
  migrations/       数据库版本迁移
frontend/           Vue 前端
deployments/docker/ 本地中间件 Compose 配置
docs/               设计、运行说明和验证记录
```

## 了解设计与当前进度

- [文档导航与学习路线](docs/README.md)：按功能设计顺序阅读。
- [后端源码导航](backend/ARCHITECTURE.md)：从请求入口追踪点赞、弹幕和恢复实现。
- [项目状态](docs/status.md)：查看已完成、待解决和下一步。
- [验证计划与报告](docs/verification/plan.md)：区分验收目标和实际测试结果。

当前以单个 API Server 为基线；在线人数展示、多 API 实例广播、完整故障覆盖和性能容量结论尚未交付。其他限制见[项目状态](docs/status.md)。

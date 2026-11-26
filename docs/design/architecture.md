# LiveStream 系统架构与模块分工

基线 V1.1。后端为 backend 内的一个 Go Module，包含 API Server 和 Worker 两个入口；第一版只运行一个 API Server。前端为独立 Vue 3 工程。

## 技术边界

HTTP 使用 CloudWeGo Hertz；WebSocket 使用 Gorilla WebSocket，经 Hertz HTTP Adaptor 接入。前端使用 Vue 3、TypeScript、Vite 和 Composition API。MySQL 8.4 保存业务与事件，Redis 7 保存实时投影及消费去重记录，Kafka 缓冲事件。

MySQL 访问统一使用 GORM。基础设施层由 GORM 创建连接池，再设置容量与管理生命周期；Repository 使用 GORM 完成业务查询和事务，Service 维护业务规则；Hertz 请求对象不进入数据访问层。结构变更通过独立的 GORM 迁移命令按版本执行，执行与恢复边界见 [数据设计](database.md#迁移与版本)。

该 WebSocket 接入组合有 [Hertz 官方支持](https://www.cloudwego.io/zh/docs/hertz/tutorials/third-party/protocol/websocket/)。依赖补丁版本在对应阶段锁定，设计选型不代表运行性能已经验证。

Hertz 通过 WithTransport 显式使用 Go 标准网络实现，使监听错误可以返回启动入口；早期检查见 [P1a 报告](../verification/reports/p1a.md)。WebSocket 现已经 adaptor 接入，连接行为的验证见[弹幕报告](../verification/reports/danmaku.md)；不同网络实现之间的性能差异尚未测量。

## 进程和数据流

浏览器通过 HTTP 使用用户、房间和查询服务，通过 WebSocket 提交弹幕和点赞。点赞：API 校验 → live.events → live-realtime / live-persistence。弹幕：身份、Redis 登记/限流、AC、冻结 → live.danmaku → danmaku-realtime / danmaku-persistence / danmaku-recent-cache。五个消费组分别维护位移；每组内的成员分担分区，不是五个独立服务。最近缓存的基本跨服务验证和未覆盖项见[验证报告](../verification/reports/recent-danmaku.md)。

读代码时先看 [API 入口](../../backend/cmd/api/main.go)怎样组装依赖，再分别看[点赞启动](../../backend/cmd/api/likes.go)、[弹幕启动](../../backend/cmd/api/danmaku.go)和 [Worker 入口](../../backend/cmd/worker/main.go)。一条用户请求从路由出发如何走到事务，可沿 [后端源码导航](../../backend/ARCHITECTURE.md) 阅读。

live-realtime 在持有 RoomHub 的 API Server 内运行，负责实时事件去重、Redis 点赞投影和广播。live-persistence 在 Worker 内运行，负责 MySQL 事件持久化和消费检查点；P4 非法事件暂停，P6 增加可靠隔离。两组都读取完整事件流，每组默认 4 个可配置成员，各自分担 16 个分区。实时成员共用同一个 API 内的连接管理器，每个活跃场次仅保留一个最高版本的待推送快照。API 每 200 毫秒广播有变化的场次总数。另一个每秒运行的任务合并名次变化，并向点赞者在本场的所有连接推送个人累计数。公共榜单按在线场次缓存最多 100 个用户名，最后一个连接离开时释放缓存。

连接注册和握手票据不经过 Kafka；在线数是尚未交付的目标。MySQL 用户及房间的 HTTP 写入由 API 完成。点赞以 Kafka Broker 确认为接受边界；仅实时消费者对 Redis 点赞投影去重和累加，Worker 独立消费并落 MySQL，Producer 不在发布前直接增加点赞数。点赞发布前不读写 Redis 请求记录；弹幕请求状态归其 Service 管理。两类事件的 `timestamp` 由客户端 UUIDv7 request_id 确定；弹幕 V2 另有冻结的服务端接收时间，供最近缓存使用。

## 模块责任

WebSocket Gateway、RoomHub 和消息处理是逻辑职责，不是已经部署的独立网关或微服务。当前 Hertz 路由和 adaptor 在同一个 API 进程接收 HTTP/WS；共享 realtime.Manager 由 API main 创建并最后关闭，握手与 readMessages 分派两项业务；audience/delivery 负责连接维护及入队，likes.Producer 负责发送，cmd/api/likes.go 的 processLike 协调消费。

| 模块或角色 | 输入与输出 | 负责 | 依赖边界 |
| --- | --- | --- | --- |
| 前端页面与状态 | 用户动作、HTTP 结果、WS 消息 → 可解释的界面状态 | 登录、房间、待确认操作、去重和重连 | 不计算权威点赞总数，不访问中间件 |
| 业务 HTTP Handler | 请求 → 响应 | 参数校验、认证上下文、错误映射 | 写业务调用 Service；弹幕历史调用专用查询 Repository，不使用 GORM |
| User Service | 注册登录、当前用户 → 用户结果 | 用户名规范化、密码哈希与校验、密码计算名额 | User Repository，不依赖 HTTP |
| User Repository | 用户查询与创建 → 数据或错误 | GORM 数据访问、数据库重复键映射 | MySQL，不定义业务响应 |
| 令牌模块与认证中间件 | 已验证账号 → JWT；Bearer → 身份 | user/token.go 签发验签，Hertz 中间件提取并传递身份 | 不从客户端 user_id 确定身份 |
| Room Service | 房间操作 → 房间状态 | 所有者权限、生命周期、准入锁 | Room Repository、RoomHub |
| WebSocket Gateway | 票据握手及消息帧 → 已认证命令 | Origin、票据、读写循环、大小与心跳 | User 身份、Room Service、消息 Service |
| RoomHub（当前由 realtime.Manager 等结构承担） | 注册、移除、广播 → 连接队列 | 慢客户端、连接与房间清理；在线数仍是目标 | 不执行数据库查询或 Kafka 发送 |
| 弹幕 Service | 已认证命令 → 接受结果 | 原请求核对、限流、AC、冻结、发布 | Repository/RequestStore、Moderator、Publisher、room/user |
| 实时消费者 | Kafka 事件 → Redis 投影与广播 | 事件校验、投影原子更新、连续位移 | Redis、RoomHub |
| Worker | Kafka 事件 → MySQL 事务 | inbox 去重、历史记录、检查点、异常事件 | Repository 与数据库事务 |
| 基础设施层 | 配置 → 客户端及生命周期 | DB、Redis、Kafka 客户端、日志与退出 | 不定义业务权限或协议 |

业务状态变更不能绕过 Service；只读历史验证房间/参数后调用专用查询 Repository。存活探针不访问业务数据；就绪探针调用 internal/database 的只读结构检查，不读取用户记录，不需要空的业务 Service 或 Repository。Hertz RequestContext 留在接入层，不进入业务与数据访问层。弹幕发布由 Publisher 接口承接，点赞保留原 Producer，前端不感知 Kafka SDK。

P1b 在 cmd/api 中组装 GORM → User Repository → User Service，并创建独立的 JWT 签发/校验组件，随后传入 HTTP 模块。Handler 负责 JSON 校验与错误映射；Service 完成密码校验后，登录 Handler 使用令牌组件签发 JWT。当前用户 Handler 只使用中间件验签得到的身份调用 Service。前端登录和刷新恢复时实际查询 users/me，再展示账号；凭证保存、期限与退出规则见 [HTTP 认证契约](api-spec.md#认证和凭据)。

P2 沿用相同分层，在入口新增 Room Repository → Room Service。HTTP 负责读取登录身份，Service 检查房主权限，Repository 在事务行锁内完成结束直播或重新开播；同轮重复关闭不改写时间，重开沿用原房间并清空关闭时间。列表按 ID 游标查询。详情通过 HTTP 读取，开放房间建立共享连接。关闭使旧场分发失效，已准入操作仍可能落库；完整在途完成屏障未实现。

参考实现中 Manager 的外部 I/O 可拖住连接注册和注销，见 [审查 ISSUE-05](../reviews/live-stream-danmaku-review.md)。消息分发层只解析和校验传输 DTO，再调用业务 Service；数据库 Model 不直接承担 WS 请求或事件契约。外部发布在房间锁之外执行，并受取消、超时和并发预算约束。

## 当前目录与后续规划

| 路径 | 职责 | 建立阶段 |
| --- | --- | --- |
| backend/go.mod、backend/go.sum | 唯一后端 Module 与锁定依赖 | P1a |
| backend/cmd/api/main.go | API 配置、组装、启动与关闭 | P1a |
| backend/cmd/migrate/main.go | 独立运行版本迁移，成功退出 0、失败退出 1 | P1b |
| backend/internal/httpserver | Hertz 实例、HTTP 路由与探针，middleware.go 放访问日志和认证限流 | P1a |
| backend/internal/config、logging | 配置合并与校验、应用日志和框架日志级别 | P1a |
| backend/cmd/worker/main.go | Worker 组装、启动与关闭 | P4 |
| backend/internal/user、room | 用户与房间业务，user/token.go 提供 JWT 和身份 | P1b、P2 |
| backend/internal/realtime | 公共连接与能力快照；danmaku*.go 管弹幕接入/分发，likes*.go 管点赞接入/恢复/推送；保持一个包 | 当前双业务连接 |
| backend/internal/likes | 事件、Kafka 收发和消费组、Redis 投影、MySQL 事务及恢复检查 | 当前点赞子链路 |
| backend/internal/database/mysql.go | GORM 连接池、迁移版本与所需结构的只读检查 | P1b |
| backend/cmd/backfill-like-board/main.go | 在停写屏障下补建已有场次的榜单 | 当前点赞榜 |
| backend/internal/danmaku | 准入、Redis 请求状态、AC、独立 Kafka、SQL 事务及历史、指标 | 本轮 P5 |
| backend/configs | 无秘密配置样例 | P1a |
| backend/migrations | 迁移调度与固定版本的 GORM 变更步骤，兼容已有成功记录 | P1b 起 |
| frontend/src、frontend/package.json | Vue 工程与检查脚本 | P1a |
| deployments/docker/compose.yaml | 统一 Compose，按阶段启用服务 | P1b 起 |
| backend/Dockerfile | API、Worker 镜像构建，尚未建立 | P7 目标 |

本轮按业务只增加一个 danmaku 包，原点赞及恢复归属保持。点赞代码集中在 likes，连接状态集中在 realtime，入口负责组装两者。代码阅读入口见 [Backend 结构](../../backend/ARCHITECTURE.md)，本轮基线与回归结果见 [精简重构验证](../verification/reports/backend-refactor.md)。P7 应用镜像仍待建立。

点赞 Kafka 循环对每条记录调用 DecodeLikeRecord，得到已校验事件、Topic 身份、分区及下一条位置。API 的 processLike 依次完成场次初始化、Redis 原子投影和在线内存同步，Worker 回调完成持久化事务，两者成功后各自提交位移。场次字段和锁在 [audience.go](../../backend/internal/realtime/audience.go)，连接帧由 [manager.go](../../backend/internal/realtime/manager.go) 的 `readMessages` 读取；[cmd/api/likes.go](../../backend/cmd/api/likes.go) 的 `runRealtimeConsumer` 负责暂停和复查，`processLike` 负责单条事件。

点赞保留原 Kafka 收发与恢复，弹幕在自己的包中维护独立收发与校验；Redis 和 MySQL 操作归仓储。本轮不额外建设 messaging 或审核微服务。外部依赖发生故障时沿用恢复设计中的处理、检查点和复查规则。

## 容量和并发边界

下表区分已实现约束和后续目标，均不构成实测容量承诺。

| 项目 | 默认上限或时间 |
| --- | --- |
| 总/单房间/单用户连接配额 | 后续目标 1000/200/5，当前未落实 |
| 单连接业务写队列 | 128 条消息 |
| 弹幕每活跃场次分发队列 | 已实现 128 条，串行分发 |
| 单连接请求处理 | 一个读循环串行调用，未实现额外 32 条请求池 |
| 待使用握手票据 | 总计 10000 个；每用户每房间 3 个仍为后续目标 |
| 单个 WebSocket 消息 | 8 KiB |
| 写入超时、心跳发送间隔、无响应超时 | 5 秒、20 秒、60 秒 |
| Worker 处理 | 成员串行处理，逐条事务及提交；单条业务预算 10 秒 |

每连接一个业务读循环和一个业务写循环；所有 ACK、错误、广播进入写队列。队列满或写超时则关闭该慢连接，不阻塞整房间。不要将每条消息转换成无上限 goroutine。RoomHub 移除连接、房间关闭和进程退出共享幂等清理路径。Gorilla 对并发读写的限制见 [包文档](https://pkg.go.dev/github.com/gorilla/websocket#hdr-Concurrency)。

API 的目录读写锁只保护 session_id 到场次对象的映射，取出引用后就释放。各场次用独立锁保护成员、个人计数、总数快照和榜单分发版本。每 200 毫秒由一个协程逐场取走待发总数和收件人列表，在锁外编码并入队。忙场次的工作留到下一轮，每秒的个人数与榜单广播使用同样的策略。榜单查询的缓存锁单独保护快照和昵称补全，释放后才登记场次的分发进度。

关闭或回收时先标记旧对象失效，按对象身份摘除目录项，再在锁外断开连接。每条连接将状态检查和非阻塞入队串行化，写协程先发 Redis 首屏，再排空握手期间积累的消息。关房使旧对象永久失效；点赞恢复保留共享连接，以新 generation/epoch 快照过滤旧状态。

弹幕已采用短临界区复制连接引用，再在锁外尝试入队；快照不延长连接的业务有效期，发送与退出竞态通过退出状态及幂等清理处理。按分发顺序向同房间连接入队，不让共享 Worker 池并行打乱该房间相邻弹幕。该思路对应 [REF-03 与 ISSUE-07](../reviews/live-stream-danmaku-review.md)；对象池等分配优化待有测量依据再引入。

同用户同房间保持分区日志顺序，不同用户可跨分区。场次串行分发统一单 API 的投递次序，不解释为墙钟全序或多 API 顺序。点赞可跨分区处理，点赞之间以及点赞与弹幕之间没有全局顺序；点赞投影依赖幂等与原子累加。每分区串行处理批次，先实现正确的连续提交，再考虑并行优化。

## 状态和可观测性

存活与就绪分开：健康接口表示进程能响应，就绪接口根据当前阶段检查必要依赖和恢复状态。依赖不可用、恢复未完成、隔离事件待处理时报告具体能力状态。

当前通过原子状态保存初始化、就绪和停止状态。P1b 的 main 调用 database.Open 创建 GORM 实例并检查连接，再调用 CheckSchema 只读检查迁移记录与所需列；通过后注入 HTTP 模块、标记就绪并启动监听。就绪探针每次执行有时限的只读结构检查，反映运行期间的数据库故障；存活探针保持独立。退出信号也能取消数据库初始化；运行时先标记停止，再交给 Hertz 关闭，最后由 database.Close 关闭底层连接池。迟到的初始化通知不能撤销停止。配置模块不依赖 Hertz，数据库模块不依赖框架请求对象。

迁移入口复用 LoadMySQL 和 database.Open，但不要求目标库已有业务表；随后由 migrations.Run 取得数据库命名锁、读取连续成功版本、顺序执行缺少的步骤。每个版本完成后才登记成功，任一步失败立即返回；结束时释放锁并关闭连接池。它是一次性维护命令，不是第三个常驻服务；API 与 Worker 均不隐含执行它。

日志关联 request_id、event_id、room_id、分区和位移；禁止记录密码、Bearer、原始握手票据。隔离日志只记消费组、Topic UUID、分区、offset、错误类别与尝试次数，原文和有界摘要由业务 Repository 保存在 failed_events。记录连接数、队列占用、接受延迟、广播延迟、持久化延迟、消费者积压、重复事件和隔离事件。完整指标验证见[验证设计](../verification/plan.md)。

弹幕 API 指标包含审核、限流、发布、提交及连接入队延迟；Worker 默认回环 9091/metrics 单独报告持久化和逐分区 lag。点赞四种恢复状态、弹幕可用性和 Worker 状态分别核对，共享依赖故障仍可能同时影响两者。

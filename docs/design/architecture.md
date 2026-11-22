# LiveStream 系统架构与模块分工

基线 V1.1。后端为 backend 内的一个 Go Module，包含 API Server 和 Worker 两个入口；第一版只运行一个 API Server。前端为独立 Vue 3 工程。

## 技术边界

HTTP 使用 CloudWeGo Hertz；WebSocket 使用 Gorilla WebSocket，经 Hertz HTTP Adaptor 接入。前端使用 Vue 3、TypeScript、Vite 和 Composition API。MySQL 8.4 保存业务与事件，Redis 7 保存实时投影及消费去重记录，Kafka 缓冲事件。

MySQL 访问统一使用 GORM。基础设施层由 GORM 创建连接池，再设置容量与管理生命周期；Repository 使用 GORM 完成业务查询和事务，Service 维护业务规则；Hertz 请求对象不进入数据访问层。结构变更通过独立的 GORM 迁移命令按版本执行，执行与恢复边界见 [数据设计](database.md#迁移与版本)。

该 WebSocket 接入组合有 [Hertz 官方支持](https://www.cloudwego.io/zh/docs/hertz/tutorials/third-party/protocol/websocket/)。依赖补丁版本在对应阶段锁定，设计选型不代表运行性能已经验证。

P1a 首步使用 Hertz 内置的 Go 标准网络实现，通过 WithTransport 显式指定。该实现将监听错误返回给启动入口，便于统一处理端口冲突；验证结果见 [P1a 报告](../verification/reports/p1a.md)。网络实现的性能差异尚未测量，后续 WebSocket 接入仍需单独验证。

## 进程和数据流

浏览器通过 HTTP 使用用户、房间和查询服务，通过 WebSocket 提交弹幕和点赞。最终事件流为：API 校验请求 → Kafka live.events → 两个独立消费者组。

live-realtime 在持有 RoomHub 的 API Server 内运行，负责实时事件去重、Redis 点赞投影和广播。live-persistence 在 Worker 内运行，负责 MySQL 事件持久化和消费检查点；P4 非法事件暂停，P6 增加可靠隔离。两组都读取完整事件流，每组默认 4 个可配置成员，各自分担 16 个分区。实时成员共用同一个 API 内的连接管理器，每个活跃场次仅保留一个最高版本的待推送快照。API 每 200 毫秒广播有变化的场次。

RoomHub 的连接、在线数和握手票据不经过 Kafka。MySQL 用户及房间的 HTTP 写入由 API 完成。点赞从 P4 首次交付起以 Kafka Broker 确认为接受边界；仅实时消费者对 Redis 点赞投影去重和累加，Worker 独立消费并落 MySQL，禁止 Producer 在发布前直接增加点赞数。发布前不读写 Redis 请求记录；事件时间由客户端 UUIDv7 request_id 确定。

## 模块责任

| 模块或角色 | 输入与输出 | 负责 | 依赖边界 |
| --- | --- | --- | --- |
| 前端页面与状态 | 用户动作、HTTP 结果、WS 消息 → 可解释的界面状态 | 登录、房间、待确认操作、去重和重连 | 不计算权威点赞总数，不访问中间件 |
| 业务 HTTP Handler | 请求 → 响应 | 参数校验、认证上下文、错误映射 | 只调用 Service |
| User Service | 注册登录、当前用户 → 用户结果 | 用户名规范化、密码哈希与校验、密码计算名额 | User Repository，不依赖 HTTP |
| User Repository | 用户查询与创建 → 数据或错误 | GORM 数据访问、数据库重复键映射 | MySQL，不定义业务响应 |
| 令牌模块与认证中间件 | 已验证账号 → JWT；Bearer → 身份 | auth 模块签发验签；Hertz 中间件提取并传递身份 | 不从客户端 user_id 确定身份 |
| Room Service | 房间操作 → 房间状态 | 所有者权限、生命周期、准入锁 | Room Repository、RoomHub |
| WebSocket Gateway | 票据握手及消息帧 → 已认证命令 | Origin、票据、读写循环、大小与心跳 | User 身份、Room Service、消息 Service |
| RoomHub | 注册、移除、广播 → 连接队列 | 在线数、慢客户端、连接与房间清理 | 不执行数据库查询或 Kafka 发送 |
| 消息 Service | 已认证命令 → 接受结果 | 限流、事件封装、发布策略 | EventPublisher |
| 实时消费者 | Kafka 事件 → Redis 投影与广播 | 事件校验、投影原子更新、连续位移 | Redis、RoomHub |
| Worker | Kafka 事件 → MySQL 事务 | inbox 去重、历史记录、检查点、异常事件 | Repository 与数据库事务 |
| 基础设施层 | 配置 → 客户端及生命周期 | DB、Redis、Kafka 客户端、日志与退出 | 不定义业务权限或协议 |

业务 Handler 和 Repository 之间不能绕过 Service。存活探针不访问业务数据；就绪探针调用 internal/database 的只读结构检查，不读取用户记录，不需要空的业务 Service 或 Repository。Hertz RequestContext 留在接入层，不进入业务与数据访问层。消息发布由 EventPublisher 边界承接，阶段演进替换实现，不让前端或 RoomHub 感知 Kafka SDK。

P1b 在 cmd/api 中组装 GORM → User Repository → User Service，并创建独立的 JWT 签发/校验组件，随后传入 HTTP 模块。Handler 负责 JSON 校验与错误映射；Service 完成密码校验后，登录 Handler 使用令牌组件签发 JWT。当前用户 Handler 只使用中间件验签得到的身份调用 Service。前端登录和刷新恢复时实际查询 users/me，再展示账号；凭证保存、期限与退出规则见 [HTTP 认证契约](api-spec.md#认证和凭据)。

P2 沿用相同分层，在入口新增 Room Repository → Room Service。HTTP 负责读取登录身份，Service 检查房主权限，Repository 在事务行锁内完成结束直播或重新开播；同轮重复关闭不改写时间，重开沿用原房间并清空关闭时间。列表按 ID 游标查询。房间详情目前只走 HTTP，尚未建立 RoomHub 或实时连接，关闭与在途发送的协调仍按领域规则后续实现。

参考实现中 Manager 的外部 I/O 可拖住连接注册和注销，见 [审查 ISSUE-05](../reviews/live-stream-danmaku-review.md)。消息分发层只解析和校验传输 DTO，再调用业务 Service；数据库 Model 不直接承担 WS 请求或事件契约。外部发布在房间锁之外执行，并受取消、超时和并发预算约束。

## 目标目录

| 路径 | 职责 | 建立阶段 |
| --- | --- | --- |
| backend/go.mod、backend/go.sum | 唯一后端 Module 与锁定依赖 | P1a |
| backend/cmd/api/main.go | API 配置、组装、启动与关闭 | P1a |
| backend/cmd/migrate/main.go | 独立运行版本迁移，成功退出 0、失败退出 1 | P1b |
| backend/internal/httpserver | Hertz 实例、HTTP 路由与探针 Handler | P1a |
| backend/internal/config、logging | 配置合并与校验、应用日志和框架日志级别 | P1a |
| backend/cmd/worker/main.go | Worker 组装、启动与关闭 | P4 |
| backend/internal/user、room | 用户与房间业务 | P1b、P2 |
| backend/internal/auth | JWT 签发和校验，不依赖 Hertz | P1b |
| backend/internal/websocket | Gateway、Hub、连接 | P3 |
| backend/internal/danmaku、interaction | 弹幕与点赞业务 | P3、P4 |
| backend/internal/messaging | 事件、发布、消费、幂等协调 | P4 点赞；P5 扩展弹幕 |
| backend/internal/database、cache、middleware | 数据访问及中间件基础设施 | 随阶段建立 |
| backend/configs | 无秘密配置样例 | P1a |
| backend/migrations | 迁移调度与固定版本的 GORM 变更步骤，兼容已有成功记录 | P1b 起 |
| frontend/src、frontend/package.json | Vue 工程与检查脚本 | P1a |
| deployments/docker/compose.yaml | 统一 Compose，按阶段启用服务 | P1b 起 |
| backend/Dockerfile | API、Worker 镜像构建 | P7 |

以上是计划目录；不为尚未使用的 Worker 或中间件创建空实现。

## 容量和并发边界

以下为初始保护默认值，可通过配置下调或根据压测调整，不构成性能承诺。

| 项目 | 默认上限或时间 |
| --- | --- |
| API 总连接数、单房间连接数、单用户连接数 | 1000、200、5 |
| 单连接业务写队列 | 128 条消息 |
| 进程事件分发队列 | 1024 条消息 |
| 单连接尚未完成的业务请求 | 32 个 |
| 待使用握手票据 | 总计 10000 个，每用户每房间 3 个 |
| 单个 WebSocket 消息 | 8 KiB |
| 写入超时、心跳发送间隔、无响应超时 | 5 秒、20 秒、60 秒 |
| Worker 每分区批次 | 最多 200 条或等待 100 毫秒 |

每连接一个业务读循环和一个业务写循环；所有 ACK、错误、广播进入写队列。队列满或写超时则关闭该慢连接，不阻塞整房间。不要将每条消息转换成无上限 goroutine。RoomHub 移除连接、房间关闭和进程退出共享幂等清理路径。Gorilla 对并发读写的限制见 [包文档](https://pkg.go.dev/github.com/gorilla/websocket#hdr-Concurrency)。

点赞定时器在连接管理器锁内取走每场最新快照，向该场连接的有界队列非阻塞入队。每条连接的写循环完成网络发送。后续弹幕广播设计采用短临界区复制连接引用，再在锁外尝试入队；快照不延长连接的业务有效期，发送与退出竞态通过退出状态及幂等清理处理。按分发顺序向同房间连接入队，不让共享 Worker 池并行打乱该房间相邻弹幕。该思路对应 [REF-03 与 ISSUE-07](../reviews/live-stream-danmaku-review.md)；对象池等分配优化待有测量依据再引入。

同一房间的弹幕顺序以其 Kafka 分区日志顺序为准，不能解释为浏览器墙钟时间的全序。点赞可跨分区处理，点赞之间以及点赞与弹幕之间没有全局顺序；点赞投影依赖幂等与原子累加。每分区串行处理批次，先实现正确的连续提交，再考虑并行优化。

## 状态和可观测性

存活与就绪分开：健康接口表示进程能响应，就绪接口根据当前阶段检查必要依赖和恢复状态。依赖不可用、恢复未完成、隔离事件待处理时报告具体能力状态。

当前通过原子状态保存初始化、就绪和停止状态。P1b 的 main 调用 database.Open 创建 GORM 实例并检查连接，再调用 CheckSchema 只读检查迁移记录与所需列；通过后注入 HTTP 模块、标记就绪并启动监听。就绪探针每次执行有时限的只读结构检查，反映运行期间的数据库故障；存活探针保持独立。退出信号也能取消数据库初始化；运行时先标记停止，再交给 Hertz 关闭，最后由 database.Close 关闭底层连接池。迟到的初始化通知不能撤销停止。配置模块不依赖 Hertz，数据库模块不依赖框架请求对象。

迁移入口复用 LoadMySQL 和 database.Open，但不要求目标库已有业务表；随后由 migrations.Run 取得数据库命名锁、读取连续成功版本、顺序执行缺少的步骤。每个版本完成后才登记成功，任一步失败立即返回；结束时释放锁并关闭连接池。它是一次性维护命令，不是第三个常驻服务；API 与未来 Worker 不隐含执行它。

日志关联 request_id、event_id、room_id、分区和位移；禁止记录密码、Bearer、原始握手票据。记录连接数、队列占用、接受延迟、广播延迟、持久化延迟、消费者积压、重复事件和隔离事件。完整指标验证见 [验证设计](../verification/plan.md)。

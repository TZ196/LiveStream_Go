# live-stream-danmaku 源码审查与设计吸收

审查日期：2026 年 11 月 19 日。本文保存参考实现的源码证据、适用思路、缺陷及我们采取的改进，供后续分阶段开发使用。当前结论来自静态阅读，没有启动参考服务、执行故障实验或复现其性能数据。

本文是设计研究记录。接口、资源参数、事务和恢复规则仍由各专属设计文档维护；实现完成情况以 [项目状态](../status.md) 为准。下面的“采用”表示设计选择或实施方向，不表示代码已经完成。

## 来源与证据边界

| 项目 | 本次观察 |
| --- | --- |
| 用户提供的本地目录 | `/home/tz196/projects/live-stream-danmaku-main` |
| Go Module | `github.com/charlesAcmen/livestream-danmaku` |
| 源码声明的主要版本 | Go 1.25.0、Gin 1.11.0、Gorilla WebSocket 1.5.3、Sarama 1.46.3、go-redis/v9 9.17.2、GORM 1.31.1；这些不是本次实测运行版本 |
| 版本定位 | 本地副本没有 `.git` 元数据，无法确定来源提交；目录名称和 Module 名不作为提交版本证明 |
| 内容指纹 | 26 个文件的清单 SHA-256：`ffb783925ba7b73caec2de21c55ac8d0ddb23e6d2fe55336f4147a84e9608124` |
| 阅读范围 | 服务与消费者入口、WS 握手及连接管理、消息分发、Redis、Kafka、Repository、Model、客户端、压测程序、Compose、README |
| 实证限制 | 未运行构建、测试、服务、压测或故障注入；该副本中未找到 `*_test.go`、前端 `package.json` 或压测原始日志、CSV 报告 |

指纹生成方式：收集目录下全部 `*.go`（排除 `.git`）及 `go.mod`、`go.sum`、`README.md`、`docker-compose.yaml`、`LICENSE`，按相对路径排序；每个文件生成一行“文件内容 SHA-256、两个空格、相对路径、LF”，再对拼接后的 UTF-8 清单计算 SHA-256。只记录摘要，不复制源码或配置中的凭据。

下文源码路径均相对于该参考目录，行号对应上述快照，同时记录函数名方便源码变化后定位。`internal/consumer/handler.go` 前 214 行为已注释旧实现，本次消费结论依据第 216 行之后的有效代码。后续参考副本改变时须重新核对相关结论；副本缺失不阻塞依据本仓库契约继续开发。

## 实际实现与适用范围

| 链路 | 源码实际行为 | 主要入口 |
| --- | --- | --- |
| 弹幕接入 | URL 提供用户及房间信息，升级 WS；读循环解析外壳并按类型分发 | `internal/api/chat_handler.go:43`，`ConnectWebSocket`；`internal/ws/client.go:42`，`ReadPump` |
| 即时广播 | Manager 将弹幕交给 Kafka 异步生产输入，再发布到房间 Redis Pub/Sub；各服务器订阅后分发到本地连接 | `internal/ws/manager.go:271`，`processDanmaku`；`:367`，`subscribeToRoom` |
| 历史存储 | 独立消费者取 Kafka 消息，经内存队列交给后台批量写库任务 | `cmd/consumer/main.go:24`；`internal/consumer/handler.go:297`，`ConsumeClaim` |
| 点赞与在线数 | 点赞先在进程内累加，再周期写 Redis；服务器上报带 TTL 的在线连接统计并汇总 | `internal/ws/router.go:60`；`internal/ws/manager.go:296`，`broadcastStats` |
| 调试和负载生成 | 命令行客户端收发消息；独立程序建立连接、发送弹幕或向 Kafka 输入数据 | `cmd/client/main.go:35`；`cmd/benchmark/main.go:53`；`cmd/benchmark/kafka/kafka.go:29` |

当前启用的业务路由主要为 `/ws`。历史查询有 Service/Repository 实现，但 HTTP Handler 被注释；没有完整的注册登录、房间权限与生命周期、Vue 前端闭环。它适合作为实时通信和吞吐实验的参考，不能由 README 架构图推断完整业务已经交付。

我们的目标仍为 Hertz 模块化单体、一个 API Server 和独立 Worker。P5 起实时消费和持久化消费读取同一 Kafka 事件流，具体职责见 [架构](../architecture.md)。这是一项已有设计取舍，增加了 Broker 确认、消费滞后与恢复管理成本；不据此宣称我们尚未实现的系统更快或更可靠。

## 值得借鉴的思路与采用方式

| 编号及实现思路 | 解决什么问题 | 我们怎么采用 | 阶段与验收 |
| --- | --- | --- | --- |
| REF-01：连接读写分离，`done` 与 `sync.Once` 协调退出。证据：`internal/ws/client.go:42/80/116` | 收发可独立推进；多个退出原因不会重复关闭同一退出信号 | 采用每连接单读、单写及幂等清理。ACK、错误、广播统一经过写队列；补齐心跳、读写超时、大小限制与回收，见 [WS 协议](../websocket-protocol.md) | P3，API/Gateway；ROOM-03、WS-04 |
| REF-02：消息外壳与类型分发。证据：`internal/model/message.go:29`、`internal/ws/dispatcher.go:8` | 新增消息类型时，不把所有处理逻辑塞进读循环 | 借鉴分发机制，沿用我们的版本、请求身份及错误契约；按类型校验输入并调用业务 Service，传输 DTO 不直接复用数据库 Model | P3/P4，API/协议；WS-03、COUNT-01、IDEM-02 |
| REF-03：锁内复制房间连接引用，锁外分发。证据：`internal/ws/manager.go:400`，`broadcastToLocalClients` | 缩短房间共享状态的锁持有时间，减少加入、退出与广播竞争 | 采用短临界区快照；允许快照中的连接已退出，发送路径检查退出状态且清理幂等。保留同房间分发顺序，不直接引入共享任务池并发广播 | P3，API/RoomHub；ROOM-03、WS-03 至 WS-05；P5 核对日志顺序 |
| REF-04：有界队列隔离接收、分发和连接写入。证据：`internal/ws/client.go:14`、`internal/ws/manager.go:69/156` | 吸收短时突发，防止单个慢连接拖住房间 | 采用容量预算与明确的满队列处理。我们的慢连接策略及初始参数以 [架构](../architecture.md) 为准；不照搬 4096 条队列或满后只丢消息的处理 | P3，API/验证；WS-04、PERF-01 |
| REF-05：按条数或等待时间触发批量写库。证据：`internal/consumer/handler.go:343`，`dbWorker` | 高负载减少数据库往返；低负载也能及时写入 | 采用双触发条件，但按分区组织有限批次，保留消息身份和位移；事务成功后推进连续位置。批次参数引用 [架构](../architecture.md)，失败规则引用 [可靠性设计](../reliability.md) | P6，Worker/数据；KAFKA-02 至 KAFKA-05 |
| REF-06：独立客户端及负载生成程序。证据：`cmd/client/main.go:35`、`cmd/benchmark/main.go:188` | 脱离页面调试协议，控制房间分布、连接和发送负载 | P3 按调试需要建立小型协议客户端；P8 建立参数化负载工具，加入身份认证、事件与接收连接标识、延迟采样、排空核对和退出回收 | P3/P8，API/验证；WS-01 至 WS-04、PERF-01、PERF-02 |

REF-01 的单读、单写与 Gorilla 的并发约束一致；同一连接的常规写操作须由应用自行协调，见 [Gorilla 包文档](https://pkg.go.dev/github.com/gorilla/websocket#hdr-Concurrency)。这解释了设计依据，不代表参考项目的所有客户端都已满足该约束。

以下思路保留为后续候选，尚未纳入 V1 实现承诺：

| 候选 | 借鉴价值 | 当前处理及再次评估条件 |
| --- | --- | --- |
| 多服务器在线数快照与 TTL，`internal/infra/redis.go:95/124` | 用定期快照和过期识别失联实例，避免永久残留计数 | V1 以本 API 有效连接为准；授权多实例设计后再处理实例身份、租约、汇总误差与广播拓扑 |
| `sync.Pool` 复用连接快照，`internal/ws/manager.go:409` | 高频广播时可能减少分配 | 先测分配与 GC；确认是瓶颈后再引入，归还前清理引用，评估池保留内存和对象所有权 |
| 合并统计广播，`internal/ws/manager.go:296` | 统计更新密集时降低下行流量 | 仅作为未来协议优化候选；不延迟权威计数到易丢失的内存批次。调整推送频率前评审版本、收敛延迟及前端行为 |

## 问题、触发条件与我们的改进

“高”表示若沿用，会影响本项目的数据、身份、顺序或资源边界；“中”表示主要影响测量可信度、可复现性和维护。源码行为可直接核对；故障后果为静态推导，尚未通过运行复现。

| 编号/优先级 | 源码事实与定位 | 触发条件及影响 | 我们的改进、设计依据与验收 |
| --- | --- | --- | --- |
| ISSUE-01 / 高：启动删表 | `internal/infra/db.go:19` 的 `InitDB` 删除弹幕、房间、用户表再迁移；API 和消费者都调用它 | 数据库账号具备权限时，启动或重启任一进程可删除已有数据 | 正常启动检查迁移版本，不隐含删表、清库或重建；清理与启动分离。[数据设计](../database.md)，P1b 起 ENV-03，P6 补 Worker |
| ISSUE-02 / 高：身份与来源未验证 | `internal/api/chat_handler.go:23/43` 接受任意 Origin，并将 URL 中的 uid/name 用作连接身份 | 调用方可修改用户 ID 或绕过业务房间准入；不能依赖前端自报身份 | 使用认证身份、一次性房间票据、Origin 和开放状态检查。[WS 协议](../websocket-protocol.md)，P3，WS-01、WS-02 |
| ISSUE-03 / 高：落库前标记消费、失败后丢批次 | `internal/consumer/handler.go:324/330` 入内存队列后 `MarkMessage`；`:413` 重试失败后返回并丢弃批次；`internal/infra/kafka.go:76` 未关闭默认自动提交 | 位移提交后、事务完成前退出会漏存；数据库持续失败时也可能继续推进已丢消息的进度 | 关闭自动提交，inbox、业务写入、checkpoint 同一事务；成功后提交连续位移。依赖故障暂停、异常事件可靠隔离。[可靠性设计](../reliability.md)，P6，KAFKA-02 至 KAFKA-04 |
| ISSUE-04 / 高：消费会话资源被跨轮复用 | `internal/consumer/handler.go:258/270/281` 只在构造时创建通道，Cleanup 关闭后 Setup 不重建；`cmd/consumer/main.go:36/64` 复用 Handler | 再均衡后新会话可能向已关闭通道发送并 panic；取消路径只处理当前缓冲，不能据此证明队列全部落库 | 会话/分区资源随所有权创建和回收；撤销后停止接收，未完成批次不提交，退出有界。[Kafka 设计](../kafka-events.md)，P5/P6，KAFKA-05 |
| ISSUE-05 / 高：连接管理循环包含中间件等待 | `internal/ws/manager.go:110/271` 在主循环中生产及发布；`internal/infra/kafka.go:116` 阻塞发送到 Producer 输入；Redis 发布等待返回 | Kafka 输入拥塞或 Redis 缓慢可拖住注册、注销及断连清理；异步 Producer 不等于调用端永不阻塞 | RoomHub 仅管理连接及分发；准入与发布独立，锁外进行可取消、有超时且有界的外部操作。[架构](../architecture.md)，P3 模拟、P5 实链路，WS-05 |
| ISSUE-06 / 高：Redis 广播与 Kafka 存储独立双写 | `internal/ws/manager.go:271` 分别走两个组件；`internal/infra/kafka.go:46` 不返回成功通知，失败仅记日志 | 可能广播成功但持久化失败，或日志已有而即时广播失败；调用方无法据此判断业务接受结果 | P5 统一 Kafka 事件入口，Broker 成功确认后 ACK，由两个消费组分别处理。仍明确广播尽力、数据库异步及 UNKNOWN 边界。[Kafka 设计](../kafka-events.md)、[可靠性设计](../reliability.md)，IDEM-03、KAFKA-01 |
| ISSUE-07 / 高：同房间顺序可能被并发打乱 | `internal/ws/manager.go:135/142` 多个 Worker 共用任务队列；`internal/infra/kafka.go:51/111` 随机分区且未设置房间 Key | 同房间后入队任务可能先发送；多个 Kafka 分区之间没有该房间的统一日志顺序 | 固定 room_id 分区；分区顺序处理，并保持进入连接队列的顺序。不同房间不要求全局有序。[架构](../architecture.md)、[Kafka 设计](../kafka-events.md)，P3/P5，WS-03 |
| ISSUE-08 / 高：点赞增量可丢失且缺少业务去重 | `internal/ws/router.go:60` 接受客户端 count；`internal/ws/manager.go:299/318/326` 先换掉累加映射，再仅处理当前活跃房间并异步递增 Redis | 写 Redis 失败、进程退出或房间先变空可能少算；重试可能多算；任意 count 也不符合我们的单次点击语义 | 每个新合法动作增加一次，重试复用身份；P4 原子接受、去重和计数，P5 起实时消费原子投影，P6 才具备完整历史恢复。[领域模型](../domain-model.md)、[可靠性设计](../reliability.md)，COUNT-01、IDEM-01、REC-01 |
| ISSUE-09 / 高：慢连接和后台任务缺少完整退出边界 | `internal/ws/client.go:42/80` 未设置消息大小和读写截止时间；`internal/ws/manager.go:156` 队列满只丢消息；`:129` 周期启动任务而无重叠限制，`:110` 无停止分支 | 慢读端可能长期占据写循环；超大输入占用内存；统计变慢可造成任务重叠，退出难以收敛 | 帧、队列、连接和后台并发均设上限；慢连接关闭，任务可取消、可等待回收，采用阶段退出预算。[架构](../architecture.md)、[部署设计](../deployment.md)，P3 起 ROOM-03、WS-04、WS-05 |
| ISSUE-10 / 中：环境与测量依据不足 | `docker-compose.yaml:7/28/35` 使用未锁定镜像标签；`internal/infra/db.go:13` 在源码中定义连接配置；README 指标缺少该副本内的原始测量记录 | 换机器或重新拉镜像难以复现；指标容易被当成普遍容量结论 | 配置由环境注入，镜像及依赖锁定；每项指标保存环境、负载、口径和原始结果。[部署设计](../deployment.md)、[验证设计](../verification.md)，ENV-01、PERF-01、PERF-02 |

ISSUE-03 的自动提交依据为 [Sarama v1.46.3 默认配置](https://raw.githubusercontent.com/IBM/sarama/v1.46.3/config.go)；`MarkMessage` 标记供后续提交的位置，并不证明数据库处理完成。[Sarama 消费会话源码](https://raw.githubusercontent.com/IBM/sarama/v1.46.3/consumer_group.go)也说明再均衡会结束旧会话并建立新会话，这构成 ISSUE-04 的生命周期依据。

ISSUE-06 中 Redis Pub/Sub 的断线消息没有补发保证，见 [Redis 投递语义](https://redis.io/docs/latest/develop/pubsub/#delivery-semantics)。我们选择 Kafka 事件流是为了统一接受和恢复依据，不代表客户端离线必达或端到端 Exactly Once。

## 性能结论的采用边界

以下仅记录参考 README 的说法及计量局限，不把它们登记为我们的实测结果，也不据静态审查断言对方所有实验结果无效。

| 参考说法或计数 | 本次能确认的依据与限制 | 我们的测量要求 |
| --- | --- | --- |
| 6 万以上静态长连接 | README 陈述；没有复现记录。当前压测代码的默认连接数或队列容量本身不证明稳定容量 | 分开记录静态连接与活跃消息负载，注明进程数、机器资源、测试时长、服务端及压测端资源 |
| 150 万 QPS 下行广播 | `cmd/benchmark/main.go:232` 累加所有连接收到的弹幕次数；属于广播扇出后的接收量 | 单独统计唯一业务接受量、事件与接收连接对的投递量、唯一落库量；例如每秒 50 个事件各发给 3 万连接，对应 150 万次接收，示例不是实测 |
| 周期丢包率小于 1% | `cmd/benchmark/main.go:137/155/299` 用预期量减当前接收量；预期量取全局连接数，差值包含在途消息，未逐个去重核对接收对象 | 按房间预定接收集合，以 event_id 和 connection_id 核对；重复、未收到、退出和在途分别记录，停发排空后在截止时间核对 |
| 上行处理速率 | `cmd/benchmark/main.go:292/298` 在客户端 WriteJSON 成功后计数，没有业务接受 ACK 核对 | 发送尝试与服务端确认接受分开，超时、拒绝和未知保留各自分类 |
| 500 万条积压两分钟内全部落库 | README 陈述；`cmd/benchmark/kafka/kafka.go:94` 在发送到异步 Producer 输入完成后结束计时，报告先于 Producer Close，未观察数据库全部提交 | Producer 入队、Broker 确认和数据库提交分别观测，落库结束由实际唯一事件集合与检查点证明，不能只看发送程序计时或 Kafka 已提交位移 |

完整口径仅维护于 [验证设计](../verification.md)；工具自身也须通过 PERF-02 的已知输入核对。优化比较要求相同业务保证、环境和负载；不能通过提前提交位移、放宽鉴权或丢弃数据来宣称同等保证下性能提升。

## 后续使用与维护

1. 进入相关阶段时，按 [开发计划](../development-plan.md) 找到本表条目，先核对当前需求、契约、已有实现与源码证据。
2. 具体任务说明采用、改造、延后或不采用哪些思路，以及场景、约束、代价和失败处理；不得仅因参考项目使用某框架、组件或参数就替换我们的基线。
3. 代码完成后，把实现路径、实际命令、证据及限制写入 [项目状态](../status.md) 或由其链接的阶段学习记录，并引用 REF/ISSUE 与验收编号。需要改变协议或重要架构时，在相应设计和决策记录中维护，不让本文形成第二套契约。
4. 对外讲解时，区分借鉴来源、自己的分析与改进、已实现内容及实测结果，遵循 [项目讲解与工程证据](../project-explanation.md)。当前只完成审查和设计吸收，没有完成上述业务能力或故障验证。

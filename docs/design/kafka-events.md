# LiveStream Kafka 事件与消费契约

基线 V1.1。点赞从 P4 首次交付即接入 Kafka、Redis 实时消费者及 MySQL 持久化消费者；弹幕从 P5 接入独立的 live.danmaku，具体启用情况见项目状态。接受与恢复规则见 [可靠性设计](reliability.md)。

先分清 Topic、消费组和组内成员：不同组各自读完整日志，组内成员分担分区。代码入口分别是 [点赞事件](../../backend/internal/likes/event.go)和[弹幕事件](../../backend/internal/danmaku/event.go)；发送及处理分别从 [点赞 Producer](../../backend/internal/likes/kafka_producer.go)、[弹幕 Producer](../../backend/internal/danmaku/kafka_producer.go)、[点赞消费者](../../backend/internal/likes/kafka_consumer.go)、[弹幕消费者](../../backend/internal/danmaku/kafka_consumer.go)追踪。

## 日志配置

| 项目 | V1 默认值 |
| --- | --- |
| 业务 Topic | 点赞 live.events；弹幕 live.danmaku |
| Broker、每 Topic 分区、复制因子 | 1、16、1；本地单 Broker |
| cleanup.policy | delete，不使用 compact |
| 消息保留期 | 7 天；不另设更短的按大小淘汰限制 |
| 单条事件大小上限 | 64 KiB；Topic 批次限制同时为 64 KiB，Producer 批次目标不超过 48 KiB，预留协议开销 |
| 消息 Key | 弹幕：`room_id:user_id`；点赞：`room_id:event_id`；均为 UTF-8 字节 |
| 分区策略 | 固定 Kafka Java 兼容的 murmur2 Key 分区；P4 验证 Go 客户端一致性 |
| Producer | 幂等启用，acks=all；单 Broker 的 min.insync.replicas=1 |
| live-realtime | 单个 API 进程内默认 4 个成员，共享连接管理器和 Redis 投影 |
| live-persistence | Worker 内默认 4 个成员，只消费 live.events 并推进本组 SQL 检查点 |
| danmaku-realtime | API 内默认 4 个成员，只消费 live.danmaku，向共享 Manager 分发 |
| danmaku-recent-cache | API 内独立消费 live.danmaku，维护场次最近五分钟 Redis 缓存；与实时组独立提交 |
| danmaku-persistence | Worker 内默认 4 个成员，只消费 live.danmaku，保存弹幕及本组 SQL 检查点 |

每组独立订阅其业务 Topic 的全部 16 个分区；点赞与弹幕不共用消费组。组内同一分区同时分给一个成员，4 个成员均衡时各负责 4 个分区。点赞有实时和持久化两组，弹幕有实时、持久化和最近缓存三组；前四组的成员数分别配置为 1–16，最近缓存组复用弹幕实时组的成员数设置。每个成员串行处理和提交自己的记录；业务组在拉取前执行各自恢复检查，缓存组有单独的可重建边界，不能把其启动视为完整性证明。

保留 Kafka 集群和 Topic 身份，禁止运行中删除重建 Topic、扩分区或更换 Key 算法。此类操作会改变恢复依据或房间分区，需要迁移设计。单节点 acks=all 不提供多副本容灾能力。

## 事件外壳

新发布的 LIKE_CLICKED 使用 schema_version=2。两个消费者同时识别版本 1 的历史单击和版本 2 的批次。未知版本按当前阶段的异常事件流程处理。

新冻结的 DANMAKU_SENT 使用 schema_version=2，增加 `server_received_at`，同时保留 UUIDv7 衍生的 `timestamp` 供身份与请求窗口校验。V1 旧弹幕继续读取，但没有可靠的服务端接收时间，不放入 V2 最近缓存；既有 V1 冻结事件重试仍按 V1 发布。下方 V1 样例保留用于兼容说明。

| 字段 | 类型与来源 |
| --- | --- |
| schema_version | DANMAKU_SENT 为 1 或 2，LIKE_CLICKED 为 1 或 2 |
| event_id | 服务端按可靠性设计生成的 UUIDv5 |
| event_type | DANMAKU_SENT 或 LIKE_CLICKED |
| request_id | 原业务 UUIDv7 |
| request_hash | 原业务指纹的 64 位小写十六进制 SHA-256 |
| room_id、session_id、user_id | 十进制字符串；来自认证与服务端校验后的场次绑定 |
| timestamp | 从客户端生成的 UUIDv7 request_id 解析出的 Unix 毫秒；重试不变，服务端校验允许时间窗口；不等于服务器接收时间 |
| server_received_at | 仅 DANMAKU_SENT V2：首次成功冻结的服务端接收 Unix 毫秒，重试复用；V1 不存在该字段 |
| moderation_version | 仅弹幕必须携带；词库及规范化策略 SHA-256，64 位小写十六进制 |
| payload | 弹幕为 content、username。点赞 v1 为 `{}`，计 1。点赞 v2 为 `{"count":5}` 形式，数量为正的 int64 |

点赞 Producer 在每次发布时设置 Kafka 记录元数据的 CreateTime。实时消费组用它统计该次发布到 Redis 返回的时间。重复记录单列，业务 JSON、分区键和去重指纹沿用原定义。

```json
{
  "schema_version": 1,
  "event_id": "a61f1c41-7c5f-514d-acb9-c9a4f6b3241a",
  "event_type": "DANMAKU_SENT",
  "request_id": "01a1edab-7800-7abc-8000-123456789abc",
  "request_hash": "ecd30c445c055a7e20f764bee1ef421ca18ecd1bcd8a92ff5b61e355f97e0fd2",
  "room_id": "1001",
  "session_id": "3001",
  "user_id": "2001",
  "timestamp": 1794988800000,
  "moderation_version": "0000000000000000000000000000000000000000000000000000000000000000",
  "payload": {"content": "大家好", "username": "alice"}
}
```

```json
{
  "schema_version": 2,
  "event_id": "2b986e4f-c298-5d56-b6d8-b7ba5b5b8c66",
  "event_type": "LIKE_CLICKED",
  "request_id": "01a1edab-7801-7abc-8000-123456789abc",
  "request_hash": "1919ead6142a9a3bfa65b3c0efdc636a08d38243afa40abb40c01e8c66cce127",
  "room_id": "1001",
  "session_id": "3001",
  "user_id": "2001",
  "timestamp": 1794988800001,
  "payload": {"count": 5}
}
```

示例审核版本只表示 64 位十六进制格式，运行时由词库和规范化规则计算。事件由原 request_id 的时间与同一业务内容确定，重试使用相同规范化序列化内容；这不是服务器首次接收时间。点赞批次由 request_id、场次与数量共同标识原始操作。旧单击与 count=1 共享原指纹。username 是首次准入时的已验证用户快照；不在消费者为每条弹幕同步查询用户名。弹幕 Key 为规范十进制 `room_id:user_id`；点赞 Key 为该 `room_id`、半角冒号 `:` 和规范小写 `event_id` 的拼接。例如 `1001:2b986e4f-c298-5d56-b6d8-b7ba5b5b8c66`。消费者根据事件类型重建并校验 Key，不一致按异常事件处置。客户端 UUIDv7 `request_id` 与服务端确定性 UUIDv5 `event_id` 仍遵循可靠性设计，不改为客户端 UUIDv4。

请求的 24 小时时间窗口用于发布准入，不用于拒绝已进入 Kafka 的延迟消费事件。消费者重算 request_hash 与 event_id 并检查外壳，不能因为 Worker 积压就丢弃旧事件。

## 接受与消费

Producer 成功回调是 Kafka 阶段 ACK 的依据；超时不等于没有写入。一个应用请求可能对应多条同 event_id 的日志记录，两个消费者组都必须业务去重。点赞沿用原有发布前不登记的规则，冲突在消费阶段停止并核对。弹幕则先使用 Redis Lua 原子登记请求、限流、审核并冻结事件；相同请求重试复用状态，变更原文、房间或场次在发布前拒绝。两项业务不共用请求状态或 SQL Inbox，客户端必须为每项新动作生成新 UUIDv7，不跨业务复用 ID。

| 消费组 | 成功处理边界 | 位移提交 |
| --- | --- | --- |
| live-realtime | 同一 Redis 脚本按事件去重，将 count 加入本场总数、个人数并更新前 100 名。有观众时记录待推送版本 | 仅提交连续完成位置，房间总数与榜单各按自己的推送周期发送 |
| live-persistence | 点赞 inbox、业务数据及 checkpoint 同一事务成功 | 事务后提交，不跨越未完成前序 |
| danmaku-realtime | 原场次串行分发完成向连接尝试非阻塞入队，更新本组 Redis 进度；无观众也可完成 | 随后提交；不等待浏览器，短期 event_id 去重最多 5000 条/活跃场次 |
| danmaku-persistence | danmaku_messages 明细/去重及 checkpoint 同一事务成功 | 事务后提交，不写点赞 inbox |

位移均记录为“下一条读取位置” next_offset，而非最后一条已处理 offset。全新且无既有投影/历史的消费组从日志最早保留位置开始；持久化组已有数据库 checkpoint 时以检查点恢复并与 Kafka 记录核对。已有数据而消费位置丢失须走恢复流程。若所需位置早于当前日志起点，报告数据缺口，禁止静默重置为 latest。

自动提交关闭。客户端 SDK 参数名可以不同，但行为必须符合上述事务边界；Java 文档中的参数名不是 Go SDK 配置文件模板。

每分区串行执行有限批次。暂不引入同分区并行写库，避免后完成的批次越过失败批次。分区被撤销时终止该分区的新工作，旧持有者不继续并行推进检查点。

任一成员处理失败时，同组成员停止拉取并等候在途处理完成。重新创建整组前核对 Redis 或 MySQL 与已提交位移。另一组独立运行。消费会话与分区所有权决定后台任务、缓冲和通道的生命周期；新会话重新建立所需资源，不复用已经关闭的通道或旧会话上下文。退出和再均衡必须在配置预算内结束，已成功事务的连续位置可以提交，未完成部分交由重放处理，Worker 不能把内存入队当成持久化成功；弹幕实时组的成功边界就是尽力分发入队，不等于网络送达。API 实时消费与 Worker 分别按 [KAFKA-05](../verification/plan.md) 核对；设计依据见参考审查 [ISSUE-03、ISSUE-04](../reviews/live-stream-danmaku-review.md)。

## 顺序与异常事件

同一用户在同一房间的弹幕在分区数及分区策略不变时进入同一分区，按该分区日志顺序处理；这不是浏览器墙钟时间的全序。点赞按事件级 Key 分散，同一房间不同点赞可进入不同分区；点赞之间以及点赞与弹幕之间不保证全局顺序。相同事件重试沿用冻结的 event_id 与 Key，分区数和算法不变时进入同一分区；仍可能产生多条记录，两个消费者必须分别按 event_id 幂等。点赞采用累加，`LIKE_UPDATE` 使用投影版本，不能仅靠到达时间覆盖界面计数。

此设计缓解 Kafka 单房间点赞 Key 热点，不保证 16 分区都由不同 Worker 并行处理，也不保证整体吞吐达到任何目标。单 Broker、单 Redis 实例及同房间计数/revision 的原子更新、RoomHub 广播和 MySQL 均可能成为下一瓶颈；先按验收计划测量，再决定是否设计 Redis 计数分片。弹幕使用 room_id:user_id，可把热门房间的不同用户分散至多个分区；单一高频用户仍受单分区与限流约束。点赞和弹幕使用不同 Topic 与消费组，但仍共享 Broker、Redis、MySQL 和进程资源；独立消费位置不等于物理资源容灾。

本地旧项目的 `live.like` 16 分区实验是独立验证，不等同于此处 `live.events`、UUIDv5 和 murmur2 契约已实现或验证。P4 创建 Topic 前固定最终分区数；运行中的 Topic 不得为扩消费者数直接扩分区或换算法，必须先设计迁移与恢复。

点赞当前对格式、版本、指纹或业务永久错误仍暂停本业务组并告警，不提交未处理位置；本轮不改变点赞恢复边界。弹幕的两个消费组已接入 V8 failed_events：同条记录最多三次尝试，退避 200、400 毫秒，共用原有十秒记录预算；永久格式/版本错误、事件冲突或明确数据约束错误可靠隔离后继续。两个组分别按消费组、Topic UUID、分区、offset 唯一登记；Worker 的异常记录与检查点共用事务，实时组隔离后推进本组 Redis 位置，两者最后才提交各自 Kafka 位移。隔离失败不提交；数据库断连、死锁和结构故障不转为坏消息。Topic 身份改变、日志缺口仍走恢复保护，不能通过隔离跳过。原文边界及修复限制见[可靠性设计](reliability.md#worker-事务与失败分类)。

Kafka 记录、检查点、Redis 投影和 MySQL 历史的保留与清理必须作为同一设计评审，不能单独缩短某一保留期。

后续可通过 k6 测量实际重复消息比例及消费者 lag，再评估是否需要额外优化；点赞不引入发布前请求登记；弹幕登记按可靠性设计实施。

## 弹幕事件的固定决策

弹幕使用 live.danmaku、danmaku-realtime、danmaku-persistence 和 danmaku-recent-cache，16 分区、各组默认 4 个成员。AC 在发布前审核；事件 payload 保存原始 content 和认证用户 username 快照，顶层 moderation_version 为词库与规范化规则的 SHA-256 版本。请求身份沿用可靠性设计。消费者不使用当前词库重新审核已接受事件。缓存组异常消息跳过，不写入持久化组隔离表；缓存组故障与恢复仍需独立验证。

同场次有界串行分发只统一本 API 内的投递次序，不按跨用户客户端时间排序。实时推送尽力送达，MySQL 历史最终一致；不能声称 WebSocket 恰好一次。弹幕不写点赞 consumer_inbox，不修改点赞恢复位移或投影版本。实际接入、故障与测量证据见[本轮报告](../verification/reports/danmaku.md)。

弹幕 Kafka 诊断头 api_received_at_ns 记录本次 Submit 开始时间，不属于冻结业务事件、不参与去重。重试重新记录本次入口时间；缺失或无效时不统计接收至入队指标，不影响业务解码。Kafka CreateTime 仍用于发布至分发/持久化延迟，二者不能混用。

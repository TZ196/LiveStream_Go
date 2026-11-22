# LiveStream Kafka 事件与消费契约

基线 V1.1。Kafka 从 P5 接入，持久化消费者从 P6 接入。接受与恢复规则见 [可靠性设计](reliability.md)。

## 日志配置

| 项目 | V1 默认值 |
| --- | --- |
| 业务 Topic | live.events |
| Broker、分区、复制因子 | 1、3、1 |
| cleanup.policy | delete，不使用 compact |
| 消息保留期 | 7 天；不另设更短的按大小淘汰限制 |
| 单条事件大小上限 | 64 KiB；Topic 批次限制同时为 64 KiB，Producer 批次目标不超过 48 KiB，预留协议开销 |
| 消息 Key | room_id 十进制字符串的 UTF-8 字节 |
| 分区策略 | 固定 Kafka Java 兼容的 murmur2 Key 分区；P5 验证 Go 客户端一致性 |
| Producer | 幂等启用，acks=all；单 Broker 的 min.insync.replicas=1 |
| live-realtime | 单 API 进程内消费，三个分区按各自顺序处理 |
| live-persistence | Worker 消费，独立消费进度 |

保留 Kafka 集群和 Topic 身份，禁止运行中删除重建 Topic、扩分区或更换 Key 算法。此类操作会改变恢复依据或房间分区，需要迁移设计。单节点 acks=all 不提供多副本容灾能力。

## 事件外壳

schema_version=1，event_type 仅允许 DANMAKU_SENT 与 LIKE_CLICKED。JSON 字段如下，未知版本进入隔离流程，不用默认值强行解码。

| 字段 | 类型与来源 |
| --- | --- |
| schema_version | 整数 1 |
| event_id | 服务端按可靠性设计生成的 UUIDv5 |
| event_type | DANMAKU_SENT 或 LIKE_CLICKED |
| request_id | 原业务 UUIDv7 |
| request_hash | 原业务指纹的 64 位小写十六进制 SHA-256 |
| room_id、user_id | 十进制字符串；来自连接及认证 |
| timestamp | 服务端首次准入时的 Unix 毫秒整数，重试不变 |
| payload | DANMAKU_SENT 为 content、username；LIKE_CLICKED 为 {} |

```json
{
  "schema_version": 1,
  "event_id": "62eac618-3791-5fc5-b572-a20981c36e6f",
  "event_type": "DANMAKU_SENT",
  "request_id": "01a1edab-7800-7abc-8000-123456789abc",
  "request_hash": "f549bd6a113a84ecc75b3c2c6d4baa9f86285da8a05730f7f9a87c035aa131fa",
  "room_id": "1001",
  "user_id": "2001",
  "timestamp": 1794988800000,
  "payload": {"content": "大家好", "username": "alice"}
}
```

```json
{
  "schema_version": 1,
  "event_id": "5e808eea-b244-55cc-98f5-c0799dd6c66d",
  "event_type": "LIKE_CLICKED",
  "request_id": "01a1edab-7801-7abc-8000-123456789abc",
  "request_hash": "f05a9c93c7267ac34e92a1553729e820c322bbac35880b298baa44ea79458b3d",
  "room_id": "1001",
  "user_id": "2001",
  "timestamp": 1794988800001,
  "payload": {}
}
```

事件冻结后重试使用原序列化内容。username 是首次准入时的已验证用户快照；不在消费者为每条弹幕同步查询用户名。topic Key 必须与事件 room_id 一致。

请求的 24 小时时间窗口用于发布准入，不用于拒绝已进入 Kafka 的延迟消费事件。消费者重算 request_hash 与 event_id 并检查外壳，不能因为 Worker 积压就丢弃旧事件。

## 接受与消费

Producer 成功回调是 Kafka 阶段 ACK 的依据；超时不等于没有写入。一个应用请求可能对应多条同 event_id 的日志记录，两个消费者组都必须业务去重。

| 消费组 | 成功处理边界 | 位移提交 |
| --- | --- | --- |
| live-realtime | Redis 原子处理成功，向当前连接的广播已尝试入队 | 仅提交连续完成位置；慢连接不阻塞提交 |
| live-persistence | inbox、业务数据及 checkpoint 同一事务成功，或异常记录可靠隔离 | 事务后提交；已知未完成的前序记录不能被后序跨过 |

位移均记录为“下一条读取位置” next_offset，而非最后一条已处理 offset。全新且无既有投影/历史的消费组从日志最早保留位置开始；持久化组已有数据库 checkpoint 时以检查点恢复并与 Kafka 记录核对。已有数据而消费位置丢失须走恢复流程。若所需位置早于当前日志起点，报告数据缺口，禁止静默重置为 latest。

自动提交关闭。客户端 SDK 参数名可以不同，但行为必须符合上述事务边界；Java 文档中的参数名不是 Go SDK 配置文件模板。

每分区串行执行有限批次。暂不引入同分区并行写库，避免后完成的批次越过失败批次。分区被撤销时终止该分区的新工作，旧持有者不继续并行推进检查点。

消费会话与分区所有权决定后台任务、缓冲和通道的生命周期；新会话重新建立所需资源，不复用已经关闭的通道或旧会话上下文。退出和再均衡必须在配置预算内结束，已成功事务的连续位置可以提交，未完成部分交由重放处理，不能把内存入队当成处理成功。API 实时消费与 Worker 分别按 [KAFKA-05](../verification/plan.md) 核对；设计依据见参考审查 [ISSUE-03、ISSUE-04](../reviews/live-stream-danmaku-review.md)。

## 顺序与异常事件

同一房间在分区数及分区策略不变时进入同一分区，按日志顺序广播。不同房间不要求全局顺序；分区顺序也不意味着终端之间的时钟顺序。LIKE_UPDATE 使用投影版本，不能仅依靠到达时间覆盖界面计数。

格式、版本、指纹或业务永久约束错误由 Worker 记录到 MySQL failed_events。该表是 V1 的持久死信存储；正常的数据库不可用不转成坏消息。实时消费者遇到异常记录暂停并暴露错误，修复和恢复见 [可靠性设计](reliability.md)。

Kafka 记录、检查点、Redis 投影和 MySQL 历史的保留与清理必须作为同一设计评审，不能单独缩短某一保留期。

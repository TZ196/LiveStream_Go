# LiveStream 数据模型与存储设计

基线 V1.1。定义表、唯一约束、索引、Redis 键及数据生命周期。本文为逻辑和物理设计，尚未创建 Migration 或执行数据库变更。

## 通用约定

MySQL 使用 InnoDB、utf8mb4，业务时间以 UTC DATETIME(3) 存储。业务主键为 BIGINT UNSIGNED，自增值对外使用字符串。UUID 使用 CHAR(36) ASCII 二进制排序，指纹使用 BINARY(32)。用户名使用规范化小写 ASCII，建立唯一索引。

外键限定为 RESTRICT，不级联删除事件；V1 不提供删除用户、房间或历史事件功能。所有 SQL 参数化。历史页按房间与 id 查询，不执行无上限全表返回。

## 业务表

| 表 | 字段及约束 | 主要索引 | 阶段 |
| --- | --- | --- | --- |
| users | id；username VARCHAR(32) NOT NULL；password_hash VARCHAR(255) NOT NULL；created_at | PRIMARY(id)，UNIQUE(username) | P1b |
| live_rooms | id；owner_id 外键 users；title VARCHAR(80)；status VARCHAR(16)，仅 OPEN/CLOSED；created_at；closed_at 可空 | PRIMARY(id)，(status,id)，(owner_id,id) | P2 |
| danmaku_messages | id；event_id；room_id 外键 live_rooms；user_id 外键 users；username VARCHAR(32) 快照；content VARCHAR(200)；created_at 事件时间；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(room_id,id) | P6 |
| interaction_events | id；event_id；room_id 外键 live_rooms；user_id 外键 users；event_type，仅 LIKE_CLICKED；created_at；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(room_id,id) | P6 |

表内业务字段除明确可空项外均为 NOT NULL。CHECK 约束确保 OPEN 的 closed_at 为空、CLOSED 的 closed_at 非空。接口校验不代替唯一约束和数据库事务。

V1 不建立 room_members 或 room_stats。在线连接完全由 RoomHub 管理；点赞汇总从 Redis 读取，恢复时从 interaction_events 唯一事件汇总。避免建立没有业务生命周期定义的成员或统计副本。

## 消费与恢复表

| 表 | 关键字段 | 约束和用途 |
| --- | --- | --- |
| consumer_inbox | event_id、event_type、request_id、request_hash、room_id、user_id、event_time、event_json JSON、first_topic_id、first_partition、first_offset、persisted_at | PRIMARY(event_id)；UNIQUE(user_id,room_id,event_type,request_id)；(event_time,event_id)；保存事件身份与完整冻结事件，支持跨业务表去重及恢复 |
| worker_checkpoints | consumer_group、topic_id、partition_id、next_offset BIGINT、updated_at | PRIMARY(consumer_group,topic_id,partition_id)；与本分区业务事务一起推进；不允许回退 |
| failed_events | topic_id、partition_id、offset、raw_key、raw_payload MEDIUMBLOB、payload_hash、error_code、error_message、status、resolution_details、attempts、first_failed_at、resolved_at 可空 | PRIMARY(topic_id,partition_id,offset)；status 为 OPEN/RESOLVED；保存不超过 64 KiB 的原始值；(status,first_failed_at) 用于处置 |
| projection_rebuilds | epoch UUID、topic_id、barrier_offsets JSON、status、started_at、completed_at 可空、reason | PRIMARY(epoch)；status 为 PREPARING/READY/FAILED；记录恢复屏障和切换状态 |

以上表在 P6 建立。consumer_inbox 的 event_json 不包含密码、令牌或票据。未知事件保留原始载荷并限制错误摘要长度，禁止将任意大异常或堆栈无限写入。

隔离记录的 RESOLVED 表示已完成原事件持久化，或经单独人工决策处理了无效记录；resolution_details 必须记录处理方式、依据和是否存在业务损失。合法事件被丢弃仍属于数据不完整，不能据此通过无损恢复验收。不得靠改状态掩盖业务未处理。

## 持久化事务

每分区检查点行按事务锁定。一个批次内按日志顺序写 consumer_inbox，首次事件再写相应业务表；重复事件比较 request_hash 和身份字段，匹配后跳过业务效果。任何非重复 SQL 错误不得用 INSERT IGNORE 一概吞掉。

业务写入或异常记录落库成功后，在同一事务中更新该分区 next_offset。事务提交后提交 Kafka 位移。Kafka 提交失败重读时，inbox 唯一约束和检查点避免重复业务数据。发生分区撤销时必须等待旧分区任务退出，再允许新持有者处理。

## Redis 键

实际键统一以 ls:v1: 开头，部署环境通过独立实例或额外环境前缀隔离。下表为逻辑键，{...} 表示替换参数，并不表示已启用 Redis Cluster。

| 逻辑键 | 类型与内容 | 生命周期 |
| --- | --- | --- |
| projection:active | String，当前 epoch | 不过期，恢复完成后切换 |
| projection:{epoch}:meta | Hash，READY 状态、topic 身份 | 不过期；恢复期间新代不对外启用 |
| projection:{epoch}:room:{room_id} | Hash，likes、revision、initialized | 不过期；已关闭房间也保留 |
| projection:{epoch}:processed:{event_id} | String，已处理标记及指纹 | 首次处理起 9 天，重复不延长 |
| projection:{epoch}:partition:{partition_id} | String，next_offset | 不过期；随投影原子更新 |
| request:{user_id}:{room_id}:{type}:{request_id} | Hash，指纹、event_id、完整事件、状态、租约、接受时间 | request_id 时间加 26 小时的绝对过期点 |
| ratelimit:danmaku:{user_id}:{window} | String，固定窗口计数 | 10 秒窗口结束后再保留 10 秒 |
| ratelimit:like:{user_id}:{window} | String，固定窗口计数 | 同上 |
| ratelimit:auth:{ip_hash}:{window} | String，登录注册计数 | 60 秒窗口结束后再保留 60 秒；P4 可接入 |

业务固定窗口按服务端 UTC 时间计算。新弹幕每用户每 10 秒最多 10 次，点赞最多 20 次；同一请求的重试先走幂等查询，不再次消耗业务额度。恶意重复帧仍受连接容量及消息大小约束。

请求创建、指纹比较、租约竞争与更新使用原子脚本；事件标记、计数、revision、分区进度在另一个原子操作中完成。脚本只处理当前请求或事件，不在脚本中遍历全库。使用 Redis 7 standalone，设置 noeviction，启用 AOF everysec 和持久卷，但不将 AOF 等同于零丢失保证。

握手票据、连接数和 RoomHub 不写入 Redis。密码、JWT 密钥不写入 Redis；请求记录包含用户业务文本，应与数据库一起限制访问。

## 容量及清理

9 天去重窗口约需每秒首次事件数乘 777600 个标记，26 小时请求记录还保存事件载荷。P4/P5 必须测量实际键大小、可用内存和拒绝阈值，不能只根据消息速率假设可以无限缓存。

达到内存预算时先拒绝新准入，已接受事件暂停消费等待资源恢复；禁止淘汰幂等键来换取吞吐。更换为有界分区快照等方案属于后续设计变更。

初始准入保护在 Redis 使用量达到 maxmemory 的 70% 时告警，80% 时拒绝新业务，并给已接受事件处理预留空间。具体 maxmemory 在 P4 按机器预算设置；全量重建前另行验证新旧两代键同时存在所需的内存，资源不足时保持停写，不能边建边淘汰。

V1 MySQL 业务历史不自动清理。Redis 旧 epoch 仅在恢复验证、切换成功并保留诊断记录后受控清理。缩短任何 TTL 或保留期前，重新验证可靠性窗口。

## 迁移与版本

按 P1b 用户、P2 房间、P6 事件及恢复表逐次增加 Migration，不改写已应用文件。每次说明前置版本、回滚限制和数据兼容性。业务约束更新同步领域模型、协议和验收；数据库备份与 Kafka 日志删除不在普通 Migration 授权之内。

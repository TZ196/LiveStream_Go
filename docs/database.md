# LiveStream 数据模型与存储设计

基线 V1.1。定义表、唯一约束、索引、Redis 键及数据生命周期。P1b 首步已实现 users 与迁移版本表，其余业务表和 Redis 键仍为后续设计；实际执行结果见 [项目状态](status.md)。

## 通用约定

MySQL 使用 InnoDB、utf8mb4，业务时间以 UTC DATETIME(3) 存储。业务主键为 BIGINT UNSIGNED，自增值对外使用字符串。UUID 使用 CHAR(36) ASCII 二进制排序，指纹使用 BINARY(32)。用户名使用规范化小写 ASCII，建立唯一索引。

外键限定为 RESTRICT，不级联删除事件；V1 不提供删除用户、房间或历史事件功能。所有 SQL 参数化。历史页按房间与 id 查询，不执行无上限全表返回。

MySQL 查询和事务统一通过 GORM 实现，业务访问集中在 Repository。模型按本文及已应用迁移映射，不因引入 GORM 就增加软删除列或其他未设计字段；采用显式字段，避免直接嵌入 gorm.Model 改变表定义。表结构由 Migration 管理，启动不调用 AutoMigrate。框架不会代替用户名唯一约束、事务边界或消费幂等设计；选型见 [GORM 决策](decisions/0004-gorm-database-access.md)。

## 业务表

| 表 | 字段及约束 | 主要索引 | 阶段 |
| --- | --- | --- | --- |
| users | id；username VARCHAR(32) NOT NULL；password_hash VARCHAR(255) NOT NULL；created_at | PRIMARY(id)，UNIQUE(username) | P1b |
| live_rooms | id；owner_id 外键 users；title VARCHAR(80)；status VARCHAR(16)，仅 OPEN/CLOSED；created_at；closed_at 可空 | PRIMARY(id)，(status,id)，(owner_id,id) | P2 |
| danmaku_messages | id；event_id；room_id 外键 live_rooms；user_id 外键 users；username VARCHAR(32) 快照；content VARCHAR(200)；created_at 事件时间；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(room_id,id) | P6 |
| interaction_events | id；event_id；room_id 外键 live_rooms；user_id 外键 users；event_type，仅 LIKE_CLICKED；created_at；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(room_id,id) | P6 |

表内业务字段除明确可空项外均为 NOT NULL。CHECK 约束确保 OPEN 的 closed_at 为空、CLOSED 的 closed_at 非空。接口校验不代替唯一约束和数据库事务。

当前 users 的 username 使用 ASCII 字符集、ascii_bin 排序和唯一索引；CHECK 限定 3–32 位小写 ASCII 字母、数字、下划线且首位为字母。User Service 按领域模型规范化输入，Repository 通过 GORM 写入，数据库唯一约束裁决并发同名注册。password_hash 保存包含参数、随机盐和输出的 Argon2id 编码；不保存明文密码。账号认证复用现有四列，未新增迁移或修改结构版本。

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

唯一日常入口是在 backend 目录执行 `go run ./cmd/migrate`。命令通过 GORM 连接目标数据库，按版本执行、成功记录、失败停止；目标库本身由 Compose 初始化。调度逻辑位于 [migrate.go](../backend/migrations/migrate.go)，只在独立命令中调用，不随 API 启动执行。

schema_migrations 使用 version BIGINT UNSIGNED 主键及 applied_at UTC DATETIME(3)，只保存成功版本和执行时间。首次执行时用 GORM 创建该记录表；成功记录必须从 1 连续递增，拒绝缺号或高于程序所支持版本的记录。已成功版本直接跳过，保留原 applied_at，不重新建表或覆盖用户数据。

| 版本 | 固定迁移步骤 | 前置条件与结果 |
| --- | --- | --- |
| 1 | [000001_create_users.go](../backend/migrations/000001_create_users.go) 使用固定 userV1 模型与 Migrator.CreateTable 建立 users | 无成功版本且不存在 users；完成后登记 1，不复用未来会变化的业务模型 |
| 2 | [000002_tighten_username_check.go](../backend/migrations/000002_tighten_username_check.go) 通过 GORM.Exec 原子替换用户名 CHECK | 已成功执行 1；校验长度、字母开头及非法字符，完成后登记 2 |

版本 2 保留已有修正原因：MySQL 正则 `$` 可以匹配末尾换行之前，版本 1 的约束因此不完整。ALTER 会检查存量数据，遇到不合规记录时失败，旧约束和版本 1 保留；需先核对并决定数据修复方式，不能自动改名或删除用户。使用单条 ALTER，避免分开删除与创建约束留下空档。

当前工作区只保留上述 Go 迁移文件。独立命令兼容此前 SQL 阶段留下的同一份成功记录：已有版本 1 只执行 2，已有版本 2 不再执行结构变更。本轮没有恢复旧 SQL 文件，也没有修改迁移步骤或数据库版本。

命令通过 GORM.Connection 在同一条连接持有数据库专属 MySQL 命名锁，名称由当前库名的 SHA1 构成；同库第二个迁移命令立即失败。每条语句使用独立 GORM 查询状态，避免失败或查询条件污染下一条语句。成功、失败和取消后均尝试释放锁，随后入口关闭连接池；连接终止也会释放命名锁。[MySQL 命名锁说明](https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html)

MySQL DDL 会隐式提交，结构变更与成功记录不是同一个可整体回滚的事务。失败或中断后先检查实际表、约束与成功记录：若某步未生效，修复原因后重试；若 DDL 已生效而记录未写入，须依据该版本逐项核对并完成受控修复后再恢复迁移。命令不会自动接管无版本的已有表、补写版本或回滚删表。不能通过强写版本号、重复整份旧 SQL 或删库重建掩盖失败。[MySQL 隐式提交说明](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html)

当前 API 要求成功记录连续且最新版本恰好为 2，并检查 users 所需列；迁移命令执行结束也进行同样的只读检查。版本过旧、过新或缺列时拒绝宣告可用。该检查不是完整结构审计，索引和约束仍需独立核对。

按 P1b 用户、P2 房间、P6 事件及恢复表逐次追加 Go Migration，不修改已应用版本。新增编号文件、在调度 switch 注册步骤，并同步 internal/database/schema.go 的 RequiredSchemaVersion；每次说明前置版本、数据影响与前向修复条件。业务约束更新同步领域模型、协议和验收；备份与日志删除不在普通 Migration 授权之内。

API 和 Worker 正常启动只校验所需迁移版本，不隐含执行删表、清库或重建；Migration 与开发数据清理有独立入口和明确操作说明。重启前后已有数据及迁移版本应保持，按 [ENV-03](verification.md) 逐阶段验证。该约束对应参考项目启动删表问题，证据见 [ISSUE-01](reviews/live-stream-danmaku-review.md)。

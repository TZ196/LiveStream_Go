# LiveStream 数据模型与存储设计

基线 V1.1。定义表、唯一约束、索引、Redis 键及数据生命周期。已实现 users、live_rooms、live_sessions、点赞事件、去重、检查点和投影初始化标记；点赞恢复采用简单有效代次记录，弹幕历史及异常隔离仍为后续设计；实际执行结果见 [项目状态](../status.md)。

## 通用约定

MySQL 使用 InnoDB、utf8mb4，业务时间以 UTC DATETIME(3) 存储。业务主键为 BIGINT UNSIGNED，自增值对外使用字符串。UUID 使用 CHAR(36) ASCII 二进制排序，指纹使用 BINARY(32)。用户名使用规范化小写 ASCII，建立唯一索引。

外键限定为 RESTRICT，不级联删除事件；V1 不提供删除用户、房间或历史事件功能。所有 SQL 参数化。历史页按房间与 id 查询，不执行无上限全表返回。

MySQL 查询和事务统一通过 GORM 实现，业务访问集中在 Repository。模型按本文及已应用迁移映射，不因引入 GORM 就增加软删除列或其他未设计字段；采用显式字段，避免直接嵌入 gorm.Model 改变表定义。表结构由 Migration 管理，启动不调用 AutoMigrate。框架不会代替用户名唯一约束、事务边界或消费幂等设计。

## 业务表

| 表 | 字段及约束 | 主要索引 | 阶段 |
| --- | --- | --- | --- |
| users | id；username VARCHAR(32) NOT NULL；password_hash VARCHAR(255) NOT NULL；created_at | PRIMARY(id)，UNIQUE(username) | P1b |
| live_rooms | id；owner_id 外键 users；title VARCHAR(80)；status VARCHAR(16)，仅 OPEN/CLOSED；created_at；closed_at 可空 | PRIMARY(id)，(status,id)，(owner_id,id) | P2 |
| live_sessions | id；room_id 外键 live_rooms；started_at、ended_at；当前活动状态 | PRIMARY(id)，(room_id,id)；每房间最多一个活动场次，需数据库约束与事务共同保证 | P3 |
| danmaku_messages | id；event_id；room_id 外键 live_rooms；session_id 外键 live_sessions；user_id 外键 users；username VARCHAR(32) 快照；content VARCHAR(200)；created_at 事件时间；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(room_id,id) | P6 |
| interaction_events | id；event_id；room_id 外键 live_rooms；session_id 外键 live_sessions；user_id 外键 users；event_type，仅 LIKE_CLICKED；click_count BIGINT NOT NULL DEFAULT 1，CHECK 大于 0；created_at；persisted_at | PRIMARY(id)，UNIQUE(event_id)，(session_id,id) | P4 |

表内业务字段除明确可空项外均为 NOT NULL。CHECK 约束确保 OPEN 的 closed_at 为空、CLOSED 的 closed_at 非空。接口校验不代替唯一约束和数据库事务。

live_rooms 是长期房间，live_sessions 记录每次独立开播。版本 4 对已有 OPEN 房间建立 started_at 为 NULL 的当前场次，不为 CLOSED 房间补造历史。创建房间、关闭及重新开播在 MySQL 事务中同时维护房间和场次；行锁及活动场次生成列唯一索引共同防止重复开播。历史迁移版本不改写。

当前 users 的 username 使用 ASCII 字符集、ascii_bin 排序和唯一索引；CHECK 限定 3–32 位小写 ASCII 字母、数字、下划线且首位为字母。User Service 按领域模型规范化输入，Repository 通过 GORM 写入，数据库唯一约束裁决并发同名注册。password_hash 保存包含参数、随机盐和输出的 Argon2id 编码；不保存明文密码。账号认证复用现有四列，未新增迁移或修改结构版本。

V1 不建立 room_members 或 room_stats。在线连接完全由 RoomHub 管理；点赞汇总从 Redis 读取，核对与恢复时对 interaction_events 按 session_id 汇总 SUM(click_count)。一行代表一个已去重的批次。避免建立没有业务生命周期定义的成员或统计副本。


### 房间与开播场次

`live_rooms` 是长期房间，每次实际从关闭转为开播时新增一个 `live_sessions.id`；首次创建即开播也创建首场。每场记录所属 room_id、开始时间、结束时间及状态；一房间最多一个活动场次。数据库应以唯一约束兜底（例如可空的活动房间生成列及唯一索引），并在同一 MySQL 事务内先锁定房间行、验证状态、创建或结束场次并更新房间状态；重试不得创建第二个活动场次。版本 4 已实现生成列唯一约束、房间行锁及相关事务；详见迁移代码。

旧数据库没有每轮开播记录，不能推断或伪造旧场次的开始、结束和点赞归属；版本 4 为旧的 OPEN 房间创建 started_at 为 NULL 的活动场次，CLOSED 房间不补造历史场次。不得修改已成功迁移或直接删除旧数据；API、WS、事件和消费者须一起携带稳定 session_id，旧场迟到事件不增加新场计数。

点赞持久化表必须对 event_id 建立独立 UNIQUE KEY；新增 session_id 并建立支持单场查询的索引（如 `(session_id, id)`）。为防止事件填写的 room_id 与 session_id 不匹配，目标 DDL 应在 live_sessions 上建立 `(room_id, id)` 唯一键，点赞事件通过 `(room_id, session_id)` 联合外键引用该键；迁移时须验证 MySQL 支持及实际约束行为。去重、业务写入和检查点在同一 MySQL 事务成功后，消费者才提交本组 offset。历史、恢复与现有房间累计语义如何迁移必须先形成可验证的方案，未完成前不能声称支持历史单场统计。

## 消费与恢复表

| 表 | 关键字段 | 约束和用途 |
| --- | --- | --- |
| consumer_inbox | event_id、event_type、request_id、request_hash、room_id、session_id、user_id、event_time、event_json JSON、first_topic_id、first_partition、first_offset、persisted_at | PRIMARY(event_id)；UNIQUE(user_id,request_id)；(event_time,event_id)；保存事件身份与完整冻结事件，支持跨业务表去重及恢复 |
| worker_checkpoints | consumer_group、topic_id、partition_id、next_offset BIGINT、updated_at | PRIMARY(consumer_group,topic_id,partition_id)；与本分区业务事务一起推进；不允许回退 |
| failed_events | topic_id、partition_id、offset、raw_key、raw_payload MEDIUMBLOB、payload_hash、error_code、error_message、status、resolution_details、attempts、first_failed_at、resolved_at 可空 | PRIMARY(topic_id,partition_id,offset)；status 为 OPEN/RESOLVED；保存不超过 64 KiB 的原始值；(status,first_failed_at) 用于处置 |
| projection_state | id=1、epoch UUID、topic_id UUID | 单行主键与 CHECK(id=1)；保存成功启用的投影代次，不存恢复任务、屏障或执行阶段 |

点赞消费所需 inbox、检查点和 interaction_events 随 P4 建立；弹幕历史与隔离按 P5/P6 交付。其中 consumer_inbox、worker_checkpoints 已随版本 4 建立，字段以实际迁移为准；projection_state 随版本 6 建立，failed_events 尚未实现。consumer_inbox 的 event_json 不包含密码、令牌或票据。未知事件保留原始载荷并限制错误摘要长度，禁止将任意大异常或堆栈无限写入。

隔离记录的 RESOLVED 表示已完成原事件持久化，或经单独人工决策处理了无效记录；resolution_details 必须记录处理方式、依据和是否存在业务损失。合法事件被丢弃仍属于数据不完整，不能据此通过无损恢复验收。不得靠改状态掩盖业务未处理。

## 持久化事务

每分区检查点行按事务锁定。一个批次内按日志顺序写 consumer_inbox，首次事件再写相应业务表；重复事件比较 request_hash 和身份字段，匹配后跳过业务效果。任何非重复 SQL 错误不得用 INSERT IGNORE 一概吞掉。

业务写入或异常记录落库成功后，在同一事务中更新该分区 next_offset。事务提交后提交 Kafka 位移。Kafka 提交失败重读时，inbox 唯一约束和检查点避免重复业务数据。发生分区撤销时必须等待旧分区任务退出，再允许新持有者处理。

## Redis 键

实际键统一以 ls:v1: 开头，部署环境通过独立实例或额外环境前缀隔离。下表为逻辑键，{...} 表示替换参数，并不表示已启用 Redis Cluster。

| 逻辑键 | 类型与内容 | 生命周期 |
| --- | --- | --- |
| projection:{epoch}:identity | String，Topic UUID 与检查通过的 Redis run_id | 不过期；运行编号变化触发数据核验，成功后重新登记 |
| projection:{epoch}:session:{session_id} | Hash，room_id、likes、revision、initialized 及榜单校验字段 board_users、board_revision、board_through | 不过期；已结束场次也保留；场次身份需从权威数据校验 |
| projection:{epoch}:session:{session_id}:users | Hash，user_id 映射到本场个人累计点击数，含全部点赞者 | 不过期，空榜不占键 |
| projection:{epoch}:session:{session_id}:top | ZSET，保存排在最前的至多 100 人 | 不过期，空榜不占键 |
| projection:{epoch}:processed:{event_id} | String，已处理标记及指纹 | 首次处理起 9 天，重复不延长 |
| projection:{epoch}:partition:{partition_id} | String，next_offset | 不过期；随投影原子更新 |
| ratelimit:danmaku:{user_id}:{window} | String，固定窗口计数 | 10 秒窗口结束后再保留 10 秒 |
| ratelimit:auth:{ip_hash}:{window} | String，登录注册计数 | 60 秒窗口结束后再保留 60 秒；P4 可接入 |

弹幕规划为按服务端 UTC 固定窗口限流，每用户每 10 秒最多 10 次。点赞入口校验正整数 count，不按点击数限流，也不配置单批点击数上限。WebSocket 帧仍适用 8 KiB 的协议大小约束。

消费端以原子脚本比较事件指纹，并更新事件标记、计数、revision 和分区进度。脚本只处理当前请求或事件，不在脚本中遍历全库。使用 Redis 7 standalone，设置 noeviction，启用 AOF everysec 和持久卷，但不将 AOF 等同于零丢失保证。

个人累计数保留有符号 64 位正整数精度。Redis ZSET 的浮点分值无法区分所有相邻的大整数，因此每个成员的 score 固定为 0。成员由 19 位左补零的点击数逐位取 9 的补数，再拼接 20 位左补零的数值用户 ID。ZRANGE 从小到大读取即为业务顺序。每次点击数增加时只需比较第 100 名、更新该用户并裁剪末位。Hash 内榜外人员的数据仍可直接读取。

board_users 记录参与人数，board_through 记录已处理至哪个场次版本，board_revision 只在前 100 名的内容变化时推进。读取和写入检查 Hash 与 ZSET 的条数、场次版本。正常运行只按变更更新，Redis 内存随各场次参与人数和事件去重标记增长。

握手票据、连接数和 RoomHub 不写入 Redis。密码、JWT 密钥不写入 Redis。

## 容量及清理

9 天去重窗口约需每秒首次事件数乘 777600 个标记。P4/P5 必须测量实际键大小、可用内存和拒绝阈值，不能只根据消息速率假设可以无限缓存。

达到内存预算时先拒绝新准入，已接受事件暂停消费等待资源恢复；禁止淘汰幂等键来换取吞吐。更换为有界分区快照等方案属于后续设计变更。

初始准入保护在 Redis 使用量达到 maxmemory 的 70% 时告警，80% 时拒绝新业务，并给已接受事件处理预留空间。具体 maxmemory 在 P4 按机器预算设置；全量重建前另行验证新旧两代键同时存在所需的内存，资源不足时保持停写，不能边建边淘汰。

V1 MySQL 业务历史不自动清理。Redis 重建写入新 epoch，成功后将有效代次存入 MySQL projection_state。旧 epoch 保留供核对，后续受控清理。恢复只针对当前单 API/Worker，不提供中途断电续建或任务接管。缩短任何 TTL 或保留期前，重新验证可靠性窗口。

## 迁移与版本

唯一日常入口是在 backend 目录执行 `go run ./cmd/migrate`。命令通过 GORM 连接目标数据库，按版本执行、成功记录、失败停止；目标库本身由 Compose 初始化。调度逻辑位于 [migrate.go](../../backend/migrations/migrate.go)，只在独立命令中调用，不随 API 启动执行。

schema_migrations 使用 version BIGINT UNSIGNED 主键及 applied_at UTC DATETIME(3)，只保存成功版本和执行时间。首次执行时用 GORM 创建该记录表；成功记录必须从 1 连续递增，拒绝缺号或高于程序所支持版本的记录。已成功版本直接跳过，保留原 applied_at，不重新建表或覆盖用户数据。

| 版本 | 固定迁移步骤 | 前置条件与结果 |
| --- | --- | --- |
| 1 | [000001_create_users.go](../../backend/migrations/000001_create_users.go) 使用固定 userV1 模型与 Migrator.CreateTable 建立 users | 无成功版本且不存在 users；完成后登记 1，不复用未来会变化的业务模型 |
| 2 | [000002_tighten_username_check.go](../../backend/migrations/000002_tighten_username_check.go) 通过 GORM.Exec 原子替换用户名 CHECK | 已成功执行 1；校验长度、字母开头及非法字符，完成后登记 2 |
| 3 | [000003_create_live_rooms.go](../../backend/migrations/000003_create_live_rooms.go) 使用固定 roomV3 建立 live_rooms、两项组合索引、房主外键和状态 CHECK | 已成功执行 2；只新增房间表，保留用户与旧成功记录，完成后登记 3 |
| 4 | [000004_create_sessions_likes.go](../../backend/migrations/000004_create_sessions_likes.go) | 新增场次、点赞事件、消费检查点和投影初始化表；旧 OPEN 房间的开始时间保留为未知 |
| 5 | [000005_like_batch_count.go](../../backend/migrations/000005_like_batch_count.go) | 为 interaction_events 增加 click_count 及正数约束，旧记录各为 1，新记录保存该批点击数 |

版本 2 保留已有修正原因：MySQL 正则 `$` 可以匹配末尾换行之前，版本 1 的约束因此不完整。ALTER 会检查存量数据，遇到不合规记录时失败，旧约束和版本 1 保留；需先核对并决定数据修复方式，不能自动改名或删除用户。使用单条 ALTER，避免分开删除与创建约束留下空档。

独立迁移命令依次补齐版本 1–6。版本 6 仅新增 projection_state 空表，首次启用点赞时兼容现有固定配置代次，随后按数据库有效代次启动。升级前停止旧进程并备份；该版本不改写业务数据。旧程序要求版本 5，升级后应使用新版本并以前向修复处理故障。由版本 4 升级时，先停旧 API、Worker 并备份，然后执行版本 5、6 迁移，已有点赞投影时先完成[本场榜单补建](../deployment.md#已有点赞数据升级到本场榜单)，再启动支持两种事件格式的新版 API、Worker。保留原 Kafka Topic、消费组位移、Redis 投影代次与去重记录。旧程序不认识新批次事件，一旦开始写入新批次，应以前向修复恢复服务。

命令通过 GORM.Connection 在同一条连接持有数据库专属 MySQL 命名锁，名称由当前库名的 SHA1 构成；同库第二个迁移命令立即失败。每条语句使用独立 GORM 查询状态，避免失败或查询条件污染下一条语句。成功、失败和取消后均尝试释放锁，随后入口关闭连接池；连接终止也会释放命名锁。[MySQL 命名锁说明](https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html)

MySQL DDL 会隐式提交，结构变更与成功记录不是同一个可整体回滚的事务。失败或中断后先检查实际表、约束与成功记录：若某步未生效，修复原因后重试；若 DDL 已生效而记录未写入，须依据该版本逐项核对并完成受控修复后再恢复迁移。命令不会自动接管无版本的已有表、补写版本或回滚删表。不能通过强写版本号、重复整份旧 SQL 或删库重建掩盖失败。[MySQL 隐式提交说明](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html)

当前 API 和 Worker 要求成功记录连续且最新版本恰好为 5，并检查用户、房间、场次和点赞相关表及 click_count 列；迁移命令执行结束也进行同样的只读检查。版本过旧、过新或缺列时拒绝宣告可用。该检查不是完整结构审计，索引和约束仍需独立核对。

按 P1b 用户、P2 房间、P3 场次、P4 点赞事件、P6 恢复表逐次追加 Go Migration，不修改已应用版本。新增编号文件、在调度 switch 注册步骤，并同步 internal/database/mysql.go 的 RequiredSchemaVersion；每次说明前置版本、数据影响与前向修复条件。业务约束更新同步领域模型、协议和验收；备份与日志删除不在普通 Migration 授权之内。

API 和 Worker 正常启动只校验所需迁移版本，不隐含执行删表、清库或重建；Migration 与开发数据清理有独立入口和明确操作说明。重启前后已有数据及迁移版本应保持，按 [ENV-03](../verification/plan.md) 逐阶段验证。该约束对应参考项目启动删表问题，证据见 [ISSUE-01](../reviews/live-stream-danmaku-review.md)。

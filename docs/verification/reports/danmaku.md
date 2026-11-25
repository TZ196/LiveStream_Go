# 弹幕、AC 审核与双业务隔离验证

2026-11-24，WSL2 Ubuntu 本地隔离环境。用户授权后，feat/redis-like-recovery 实际合入 main/c800921 并推送；feat/danmaku-ac 从该提交继续，基础提交 8cbe0f8，完整链路提交 94d117f，本报告随同一任务分支提交。不自动合并弹幕，不删除原恢复分支。

## 实现与取舍

只新增 internal/danmaku 业务包、V7 表及必要装配/前端。点赞 Topic、分区键、Lua、SQL 恢复逻辑和迁移 1–6 与 c800921 比较无差异；改动限于共享 Manager 生命周期、准入及恢复快照。

两 Topic/四组；弹幕按 room_id:user_id 分区，AC 在发布前运行。Redis 状态负责限流及冻结，danmaku_messages 自身负责最终去重，检查点按组/Topic UUID/分区分开。无新增 Outbox、Inbox、恢复任务表或审核组。

参考吸收 REF-03/04、ISSUE-04/05/06/07：短锁复制连接、锁外非阻塞分发、有界队列、统一 Kafka 接受边界、独立退出。按用户选择保留同用户日志顺序与单 API 场次投递次序，取消跨用户房间全序；批量写库优化延后，先逐条事务。契约分别维护于专属设计。

## 环境与隔离

i7-13620H，WSL 可见 16 逻辑 CPU；Go 1.26.8、Node 26.13.0。MySQL 8.4.13、Redis 7.4.7（AOF/noeviction）、Kafka 4.0.0 KRaft 单 Broker；两 Topic 各 16 分区，每组 4 成员。API、Worker、Go 集成测试均使用 race 构建或检测。

专用 ls-danmaku-mysql/redis/kafka 容器有 livestream.test.owner=danmaku-ac 标签，端口 33306/36379/39092；API 38080、Vite 35173、Worker 指标 39091。故障模式使用新库及重建的隔离 Topic/Redis，操作前验证归属。原 livestream-* 数据卷和用户开发库未修改。随机测试凭据只保存在本地私有临时配置，不提交。测试完成后已停止隔离 API/Worker/Vite，删除本轮容器、匿名卷、网络及浏览器临时资料，确认 Docker Desktop 与后端进程关闭；原开发库未迁移。

## 功能与故障结果

| 范围 | 实际结果 |
| --- | --- |
| DAN-01 | AC 中文、后缀/重叠、NFKC/大小写、指定零宽字符、正常语料、只读并发、词库错误与稳定版本通过；80 条人工词有分类和整理依据 |
| DAN-02/03 | 16 个并发相同请求仅发布同一事件；拒绝和重试只占一次额度，第 11 个新请求限流；Broker 已接受但调用未确认，词库更新后仍复用原事件/版本 |
| DAN-04 | 双组分别处理完整事件；重复不重复落库，内容冲突/外键失败不推进检查点；缓存丢失可用 SQL 证明，缺冻结记录和 SQL 时不重造 |
| DAN-05 | 正常、点赞关闭、缺词库、点赞坏消息四种真实 API/Worker 模式均通过；共享握手、业务能力、普通 API 就绪分别工作 |
| DAN-06 | 两用户各 5 条并发消息保持各自顺序，两个客户端投递顺序一致；跨房间和旧场重放隔离通过；单元/race 另测 40 条并发分发与慢连接满队列 |
| DAN-07 | 分别用隔离库触发器使弹幕/点赞事务失败，本组明细/checkpoint 不动，另一组继续持久化；修复后原记录完成 |
| DAN-08 | 前端 17 项：原 ID 重试、拒绝编辑、历史核对、重连补查、刷新/换场、Unicode、恢复暂存与旧快照过滤 |
| 原点赞恢复 | 真实正常 Redis 重启保留 epoch/计数；旧 AOF 回退、清空与 Worker 积压后重建，5 个唯一批次/21 点击不重复 |
| 原连接恢复 | 正常重启即使 epoch 不变，原连接仍收到更高 generation 快照并继续发送；异常恢复期间新建的连接也无需手动重连 |
| 原点赞 API | 101 唯一事件/305 点击、ACK 重试、本人多连接、榜单、重连及新场通过；另跑 Worker 停启与 Redis 暂停/恢复通过 |

首次真实落库发现 GORM 写入零值 persisted_at，被 MySQL 严格模式拒绝。改为数据库默认 CURRENT_TIMESTAMP(3) 后，在全新库/Topic 重跑 9 个核心集成子用例全部通过，未把失败轮当作通过。

## 真实浏览器

使用已安装的 Windows HeadlessChrome 157 和 CDP 驱动隔离 Vite，实际页面通过普通文本、200 个 emoji、违规拒绝后修改、HTML 测试文本纯文字显示、历史去重、断线重连、唯一活动房间 socket 及点赞。Vite HMR 连接不算业务连接。

9 条成功弹幕，以同一 performance.now 从 WebSocket.send 测到对应广播 message 回调：P50 20.40 ms、P95/P99 25.90 ms。样本很小，仅保留汇总日志，未逐条保存时序；不能作为稳态性能或生产容量，也不等于 Vue 绘制完成时间。

## 短时负载与指标

30 个已签发测试身份的连接同时各发 4 条；24 用户在热门房间、6 用户在另一房间。直接建立无有效登录密码的隔离用户，避免注册限流混入弹幕口径，未绕过生产业务准入。120 条均在发送连接获得 ACK 和相应广播，最终唯一落库 120，无拒绝/未决；未统计所有接收连接对的覆盖率。

全部请求确认与自身广播完成耗时 439.92 ms，120/该窗口为 272.78 条/秒。API 此前已运行浏览器测试，无独立稳态/预热，未测资源峰值，不代表最大吞吐。Go WS 客户端 ACK P50/P95/P99 为 160.29/268.08/269.18 ms，广播接收为 230.96/396.51/424.67 ms；此项不是浏览器指标。

独立只读拉取日志核对实际分区及 Java murmur2，未创建第五组或改位移。热门房间 96 条实际分布：

| Partition | 0 | 1 | 3 | 4 | 6 | 8 | 9 | 11 | 12 | 13 | 15 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 条数 | 8 | 20 | 4 | 8 | 4 | 4 | 12 | 16 | 4 | 8 | 8 |

负载前后 Prometheus 桶增量，各阶段 120 样本。以下为分位所在桶的**上界（ms）**，不是精确分位：

| 指标 | P50 ≤ | P95 ≤ | P99 ≤ |
| --- | ---: | ---: | ---: |
| AC 审核 | 0.1 | 0.1 | 1 |
| Redis 登记/限流 | 5 | 5 | 5 |
| Kafka 发布调用 | 25 | 100 | 100 |
| Submit 处理 | 100 | 100 | 100 |
| Kafka 至连接队列入队结束 | 100 | 250 | 250 |
| API 接收至连接队列入队结束 | 250 | 250 | 500 |
| Kafka 至 MySQL 事务成功 | 500 | 1000 | 1000 |

排空 3 秒后，弹幕两组全部分区 lag=0。各阶段 P99 不能相加，入队不等于浏览器收到。本机临时证据在 /tmp/livestream-danmaku：core.log、各模式 api-test.log、regression-*.log、load.log、load-metrics.json、browser.log。本报告保存范围及汇总，临时路径不是跨机器可用的归档。

## 检查与复现

gofmt、go vet ./...、go test ./...、go test -race ./...、三个主入口构建、go mod verify 均通过。无测试文件的包未执行用例；普通测试的外部用例按显式开关跳过，集成结果来自另行实际执行。前端 17 测试、type-check、build 通过；Compose config 通过且中间件确实运行用于集成。产物和凭据不提交。

先准备独立 Topic/四组、新库与 Redis，迁移 1–7 并启动 API/Worker，配置 TEST_API、TEST_MYSQL_DSN、TEST_REDIS、TEST_KAFKA（均加 LIVESTREAM_ 前缀）及相同 WS_ORIGIN。从 backend 执行：

~~~sh
LIVESTREAM_TEST_DANMAKU=1 go test -race -count=1 -v ./internal/danmaku -run '^TestDanmakuIntegration$'
LIVESTREAM_TEST_DANMAKU_API=normal go test -race -count=1 -v ./internal/realtime -run '^TestDanmakuAPIIntegration$'
LIVESTREAM_TEST_DANMAKU_LOAD=1 go test -race -count=1 -v ./internal/realtime -run '^TestDanmakuLoadIntegration$'
~~~

核心集成第一条自行迁移，须使用无业务的新库、空 Redis DB15 和独立 Topic，不能与运行中的应用共用。四种 API 模式 normal/likes-disabled/words-missing/like-poison 各自重建环境，并对应配置点赞开关和词库。负载还需相同测试 JWT 密钥。Redis 回归依赖外部控制进程实际停启/暂停 Redis、复制旧 AOF、停启 Worker；本轮控制脚本在同一私有临时目录，不作为通用部署工具提交。

## 限制

在线广播尽力、历史最终一致，单 API 承载房间；无跨用户 Kafka 全序、多实例顺序或离线必达保证。AC 仅匹配当前词库，非完整内容安全系统。短期广播去重最多 5000 条/活跃场次，前端仍去重。

Redis 请求身份变化可能有五分钟保守保护，即使 AOF 看似完整；普通 API 重启沿用同一 Redis 身份无需等待。缺原冻结记录和 SQL 结果的旧请求可长期未确认，无耐久请求账本或 Outbox。点赞恢复中再次断电、任务接管和断点续建仍不在范围。

未做长时稳态容量、跨实例、真实公网故障或完整 Kafka 多轮再均衡压力；全局连接配额、完整在线人数及 failed_events 持久隔离未交付。不宣称整个 V1/P8 完成。

## 2026-11-25 同步等待告警复查

gopls 复现 manager.go 的两处 SA2001。原空临界区用于等待既有操作完成，不能删除锁；改为两个明确的等待方法，各自在返回前 defer 解锁，维持“先释放共享房间锁，再等待旧点赞发布”的原顺序，没有屏蔽分析器或改变恢复边界。

在已有 likes_test.go 增加 TestPauseWaitsForExistingOperations，核对暂停确实等待旧房间操作和点赞、等待点赞时共享锁可用、结束后状态保持 RECOVERING；原写法及整理后的写法分别通过该 race 用例。整理后 realtime 全部单元/race、go vet ./...、go test ./...、API/migrate/Worker 构建均通过；gopls 对修改文件无诊断，Staticcheck v0.8.1 的 SA2001 专项通过。

本次未重跑真实中间件集成；未更改协议、迁移、限流口径、历史数量或目录布局，测试文件保留。

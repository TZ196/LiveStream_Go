# 点赞重启与恢复验证

日期：2026-11-24。基于 main `4924618`，在任务分支 `feat/redis-like-recovery` 实施。恢复边界以[可靠性设计](../../design/reliability.md)、[数据设计](../../design/database.md)、[HTTP](../../design/api-spec.md)和[WebSocket](../../design/websocket-protocol.md)为准。

## 范围与实现

保持一个 API、一个 Worker 进程与两个独立消费组。正常 Redis 重启检查实例、场次计数、个人数、榜单及去重依据，完整数据继续使用原 epoch。逐分区允许 Redis 领先 Kafka 提交位置并幂等重放，落后或缺失时转重建。消费位置超出保留范围时拒绝启动，不让 SDK 静默跳到保留起点。

异常恢复串行执行：停止后端准入、等待在途发布、排空 Producer、退出实时组、记录各分区 B、等待 Worker 追平、核对明细和 inbox、从一致快照构建新 epoch、恢复必要标记、校验后对齐实时位移并保存有效代次。场次从 live_sessions 枚举，零赞场次保留。Migration 6 只增加 projection_state，历史迁移 1–5 未改写。

前端按用户/房间/场次暂存未发送累计数和冻结请求，恢复期间继续采集。首次发送前生成并保存请求，ACK 丢失或重连使用原 ID 与数量；收到有效同场首屏后限速补发。普通 API 就绪与点赞 CHECKING/READY/RECOVERING/BLOCKED 分开，Worker 独立报告状态。

参考审查关联：ISSUE-03 的事务后提交边界、ISSUE-04 的消费会话资源回收、ISSUE-08 的可靠计数与去重。这里验证点赞恢复，不表示完整弹幕、异常隔离或端到端 Exactly Once 已交付。

## 环境与方法

WSL/Linux amd64，Go 1.26.8、Node 26.13.0；MySQL 8.4.13、Redis 7.4.7、Kafka 4.0.0。独立容器 `ls-recovery-mysql`、`ls-recovery-redis`、`ls-recovery-kafka`，宿主端口 23306/26379/29094；API 为 28080，Origin 为 25173。每轮使用新测试库、独立空 Topic 和随机投影代次。未在原开发数据上进行删除、回退或故障注入。

普通检查：backend 执行 gofmt、`go vet ./...`、`go test ./...`、`go test -race ./...`，构建 api、worker、migrate、backfill-like-board。frontend 执行 `npm run type-check`、`npm test`、`npm run build`。默认 Go 测试中外部集成案例会跳过；cmd、config、database、httpserver 等显示 no test files 的包没有执行用例。实际中间件案例另以显式测试环境运行。

原始日志及隔离控制脚本在本机 `/tmp/livestream-like-recovery/`，包括 core-race.log、e2e.log、dependencies.log、existing-api-regression.log、api.log。临时目录不是项目运行依赖，移除后不能当作仍然存在的历史原始证据。

## 实际结果

| 检查 | 结果与证据 |
| --- | --- |
| 普通重启与 Redis 领先 | Check 保持原 epoch，重复消息只产生 12 赞；模拟旧运行编号及运行中旧编号写入拒绝均通过 |
| 去重键缺失 | 领先区间和相等位移两种情况均拒绝直接继续，转入恢复 |
| Worker 积压 | 停止 Worker 后重建等待；Worker 恢复后补齐，重建得到总数 23、本人 16、第二人 7；零赞场次仍为 0 |
| 重建后重试 | 旧请求重放不加数，新批次 3 赞正确得到 26 |
| 不完整来源 | click_count 与冻结事件不一致、缺少 inbox、所需位置早于 Kafka 日志起点均返回恢复条件不足 |
| 真实 Redis 重启 | 从持久卷 AOF 加载后重新核验，保持原 epoch 和 7 赞 |
| 真实旧 AOF 回退 | 保存 7 赞时 AOF，继续接受并落库到 12，停 Redis 后还原旧 AOF；自动构建新 epoch 恢复到 12，重试旧 5 赞仍为 12 |
| Redis 完全丢失 | Worker 停止时已接受的 4 赞留在 Kafka；清空隔离 Redis 后保持 RECOVERING，Worker 恢复后完整重建到 19，旧请求重放不增，再加 2 得到 21；最终 5 个唯一批次 |
| 普通 API 分离 | 上述 RECOVERING 窗口中 /readyz、房间查询、实际注册和登录正常，票据返回 503 |
| 原链路回归 | TestLikeAPIIntegration：101 个唯一事件、305 次点击，同场重试、重连、个人榜与重新开播均通过 |
| 依赖暂停回归 | TestLikeDependencyRecoveryIntegration：Worker 停启、Redis pause/unpause、在途结果未知后原请求补发均通过 |
| 竞态检查 | 恢复仓储集成、真实 API 三项集成及普通包测试均使用 -race，通过 |
| 前端 | 6 项用例通过：恢复期间 130 次点击和刷新补发、ACK 丢失后 9+4 精确计数、场次隔离、等待首屏、超过 JS 安全整数拆批、损坏缓存保留 |

真实进程恢复用例最近一轮为 42.90 秒，覆盖多次依赖控制与两次重建；这是整项测试耗时，不是单次恢复性能指标。原链路 100 个新批次的本地样本中 ACK p95=17.22ms、p99=21.46ms，快照 p95=210.19ms、p99=217.06ms；仅是隔离单机功能回归记录，不是容量或公网时延承诺。

## 复现入口

先准备上述隔离服务和全新测试库；不能把开发配置直接作为 TEST 配置使用。基础仓储恢复测试会执行迁移，要求空业务表和空的 16 分区 live.events：

```sh
cd backend
# 显式设置隔离的 LIVESTREAM_TEST_KAFKA、LIVESTREAM_TEST_REDIS、LIVESTREAM_TEST_MYSQL_DSN。
LIVESTREAM_TEST_RECOVERY=1 go test -race ./internal/likes -run '^TestRecoveryIntegration$' -count=1 -v
```

真实进程测试先在另一套空库/Topic 启动本分支 API 和 Worker，设置 LIVESTREAM_TEST_API、LIVESTREAM_WS_ORIGIN 及上述 TEST 变量。控制目录使用新空目录，外部控制器按下表在 reached 文件出现后执行操作，再创建 proceed 文件：

| reached | 隔离环境操作 | proceed |
| --- | --- | --- |
| capture-aof | 正常停止测试 Redis，复制 /data/appendonlydir 到新备份目录，重新启动 | redis-restarted |
| rollback-aof | 停止 Redis，将上述备份内容复制回 /data/appendonlydir，再启动 | aof-restored |
| stop-worker | 正常停止测试 Worker | worker-stopped |
| lose-redis | 仅对测试 Redis 执行 FLUSHALL | redis-cleared |
| waiting-for-worker | 启动原测试 Worker | worker-resumed |

```sh
LIVESTREAM_TEST_AUTOMATIC_RECOVERY=1 go test -race ./internal/realtime -run '^TestAutomaticRedisRecoveryIntegration$' -count=1 -v
# 退出控制目录配置后，在正常测试进程上检查原点赞链路：
go test -race ./internal/realtime -run '^TestLikeAPIIntegration$' -count=1 -v
cd ../frontend
npm run type-check
npm test
npm run build
```

完整自动控制器的本轮副本为 `/tmp/livestream-like-recovery/run-e2e.py`，它创建新测试库、构建并启动隔离应用、执行上述操作，最后退出测试进程；依赖容器由单独清理步骤回收。前端用例使用真实 composable 加受控浏览器传输/存储，不等同于执行了真实浏览器长时间离线测试。

## 交付边界

不包含恢复途中再次断电、任务接管或断点续建，不增加恢复任务表。数据恢复以已持久化明细或仍保留的必要日志为前提。旧的已发送请求超过原 24 小时窗口仍保留未确认提示，不改 ID 伪装新请求。依赖实际错误的完整处置、弹幕与在线人数仍按后续阶段推进。

2026-11-24 16:50（Asia/Shanghai）已将本地 API、Worker、Vite 切换至任务分支实现提交 f7e8815。API 与 Worker 从该提交的干净工作区构建，实际二进制 vcs.modified=false。API/Worker/前端三个用户服务均 active/running；8080 与前端代理 5173 的 /readyz 均返回 ready，点赞 recovery=READY。

切换前已保存单事务数据库备份：`/home/tz196/.local/state/livestream-go/backups/like-recovery-20261124-165003/livestream-before-v6.sql`，目录权限 0700、文件 0600。执行独立 migrate 仅新增版本 6；.env 未改，取消了此前运行 API 时临时关闭点赞的进程覆盖。MySQL 两个场次切换前后均为 149 赞/82 条、19 赞/6 条；有效 epoch 仍为原 a1c2e3c4-f88e-4618-a8fd-6d3480db8988，正常核验后继续使用，没有清零或全量重建。两个组各 16 分区、4 个活跃成员，lag=0。

前端页面与代理访问 200，未知账号登录返回预期 INVALID_CREDENTIALS/401；未使用未知密码冒充成功登录，实际成功注册登录已在隔离进程验证。当前浏览器入口为 http://127.0.0.1:5173 。本地数据库现在为版本 6，旧 main 版本 5 程序不能直接连接此结构；当前服务应使用本任务分支程序。

本轮创建的三个隔离测试容器及其匿名卷已清理；原开发 MySQL、Redis、Kafka 容器与数据卷保持。测试日志和控制脚本继续留在上述临时目录供本机审阅。

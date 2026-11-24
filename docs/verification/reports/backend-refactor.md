# Backend 精简重构验证

2026 年 11 月 24 日。功能基线为 `567c2f7`，本任务先补回归用例，再按点赞、身份与基础文件、实时入口四部分提交。

## 环境与证据

WSL2 Ubuntu，Linux/amd64，Go 1.26.8，CGO 开启，宿主 CPU 为 i7-13620H，WSL 可见 16 个逻辑 CPU、7.6 GiB 内存。使用本轮新建的 `ls-refactor-mysql`、`ls-refactor-redis`、`ls-refactor-kafka`，版本分别为 MySQL 8.4.13、Redis 7.4.7、Kafka 4.0.0。回环端口分别为 23306、26379、29092，API 为 28080。Topic 为独立的 `live.events`，16 分区、单副本，两组默认各 4 个成员。

测试凭据随机生成并只放在本机临时目录。原始日志、运行环境和测试用的进程管理脚本位于 `/tmp/livestream-refactor-mxy426cz/`。临时目录的保留时间取决于本机清理策略，下表记录本轮实际结果。

## 重构前基线

| 检查 | 实际结果 |
| --- | --- |
| `go vet ./...` | 通过 |
| `go test -count=1 -v ./...`，配置隔离 Kafka、Redis、MySQL，暂不设置 TEST_API | 11 项通过，实际包含 5 项中间件集成；真实 API 用例单独运行 |
| `go test -race -count=1 ./...`，无外部依赖环境变量 | 6 项通过；5 项存储集成及 API 用例跳过 |
| `go build` API、Worker、migrate、backfill-like-board | 4 个命令通过，输出到临时目录；独立空库按 1→5 迁移成功 |
| `TestLikeAPIIntegration` | 通过，实际启动基线 API 和 Worker，经 HTTP 注册登录与一次性票据建立 WebSocket；重复发送同一批 5 次点击，仍累计 5，随后 100 个唯一批次每批 3 次；本场 SQL 最终 101 条、305 次，榜单与个人数为 305 |
| 连接与并发回归 | 真实 API 验证本人另一连接收到个人更新、其他用户只收公开变化、重连首帧为 Redis 首屏、关闭并重新开播使用新场次。单元回归验证等锁超时重放补内存、同版本只推一次、满 128 项的慢连接不阻塞健康连接、忙场次延后而其他场次继续、旧对象清理不删除新对象 |
| 退出与生成产物 | 基线 API/Worker 收到 SIGTERM 后在 10 秒预算内退出。原 `backend/bin/` 的 4 个未运行编译产物已清理，可由命令重新构建 |

基线日志为 `baseline-tests-raw.log`、`baseline-api-tests.log`、`baseline-migrate.log` 和 `baseline-api.log`、`baseline-worker.log`。存储测试会制造预期重复键，GORM 的重复键日志与用例通过状态一起核对。

## 负载口径

`TestLikeAPIIntegration` 使用同一主机、1 个房间、两个用户，先验证 3 条连接，再用发送者重连后的连接测量。先发送两条相同的 5 点击记录，再测 100 个唯一批次，每批 3 次，等待对应 ACK 后间隔 20 毫秒。记录从发送到首次 ACK 的时间。绝对总数快照覆盖的每个批次以同一客户端单调时钟记录首次可见时间；这里的接收端是 Go WebSocket 客户端。测量为短时回归样例，后续 P8 按完整验证设计测容量和长时间负载。

| 版本与轮次 | ACK 样本 | ACK p95 / p99 | 快照样本 | 快照可见 p95 / p99 |
| --- | --- | --- | --- | --- |
| 基线，第 1 轮 | 100 | 15.229416 / 15.586443 ms | 100 | 206.053921 / 216.280795 ms |

## 复现入口

点赞包阶段的 `go vet ./...`、普通测试和四命令构建通过。隔离环境执行 `go test -count=1 -v ./...` 及 `go test -race -count=1 -v ./...`，两轮各有 12 项通过，包含 6 项中间件用例，真实 API 用例留给整体运行单独执行。新增坏记录入口用例证实业务回调未触发且 Kafka 位移未提交；事务中插入不存在场次的事件时，inbox 和检查点一起回滚。日志为 `likes-tests-raw.log`、`likes-race-raw.log`。

身份与基础文件阶段通过格式、vet、普通测试、race 单元回归和四命令构建。JWT 实现与基线逐字节比较，仅包声明改变。独立 `refactor_empty` 空库实际迁移 1→5，重复迁移成功；将该隔离库的 users 表改名后，API 在监听前以退出码 1 拒绝并报告结构缺失。实际 API/Worker 复跑完整点赞入口用例通过，结果仍为 101 条事件、305 次点击，证据为 `user-api-tests.log`、`user-migrate-0.log`、`user-migrate-1.log`、`user-schema-rejection.log`。API/Worker SIGTERM 退出通过。

在 `backend/` 执行格式检查、`go vet ./...`、`go test -count=1 -v ./...`、`go test -race -count=1 ./...`、`go build ./cmd/...`。中间件用例读取 `LIVESTREAM_TEST_KAFKA`、`LIVESTREAM_TEST_REDIS`、`LIVESTREAM_TEST_MYSQL_DSN`。

真实 API 用例另设 `LIVESTREAM_TEST_API` 和 `LIVESTREAM_WS_ORIGIN`，在全新隔离数据及 Topic 上运行迁移、启动 API 与 Worker，然后执行 `go test -count=1 -v ./internal/realtime -run '^TestLikeAPIIntegration$'`。原始存储用例与真实 API 用例分别使用独立的初始日志和数据库，避免两种测试生成的记录相互影响。

案例关联 IDEM-01、KAFKA-01/02/03/05、WS-01/04/05、UI-01 和参考审查 ISSUE-03/04/05/07。各案例的覆盖以表中实际操作为准。完整 Redis 丢失重建、异常事件持久隔离、关房在途完成屏障与公网长时间负载继续按项目状态维护。

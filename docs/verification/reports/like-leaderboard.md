# 本场点赞榜与存量补建验收

2026 年 11 月 22 日，在 WSL2 的两个隔离 Compose 项目完成 BOARD-01 至 BOARD-03。一个项目运行 Go 集成测试，另一个运行旧版与新版 API、Worker、Vite 以及 Chromium。使用 Go 1.26.8、Node 26.13.0、MySQL 8.4.13、Redis 7.4.7 和 Apache Kafka 4.0.0。Kafka 为单 Broker、16 个分区，两组各 4 个成员。

## 历史数据与升级

使用批量点赞版本 `dcb1b4c` 的旧程序在同一场次产生 101 个唯一批次。99 位用户各点 100 次，Alice 点 5 次，Bob 点 1 次，同时重复投递 Bob 的批次。Redis 总数和 MySQL 的 SUM(click_count) 均为 **9,906**。

| 实际操作 | 观察结果 |
| --- | --- |
| 实时消费组运行时补建 | 命令提示先停止 live-realtime，退出失败 |
| 停止 Worker 后再投一条重复记录，待实时组追平并退出 | 持久化组有一个分区落后 1 条，补建命令报出分区、提交位置与日志末端 |
| 旧 Worker 追平退出，直接启动新版 API | 检测到旧投影缺榜，启动失败并提示先补建 |
| 停写后运行两次补建，再启动新版 API | 两次均成功。旧场仍是 9,906 赞，公开 100 行。同为 100 赞的 99 人按数值 ID 升序。Alice 第 100，Bob 的个人响应只有 1 赞 |
| 在 Redis 集成测试中丢失个人 Hash、输入错误历史总数或不完整的暂存榜 | 残缺投影拒绝读取和累加，补建切换失败时原榜可读 |

## 浏览器和端到端校验

三个登录用户共打开五个房间页面。Bob 使用同一账号的两个标签页，另有一个页面进入独立房间。

| 实际操作 | 观察结果 |
| --- | --- |
| Bob 从 1 点到 3，仍在榜外 | 两个标签页都显示“我的点赞：3”。其他用户收到 0 条个人消息，公共榜单增量为 0 |
| Bob 再点 3 次，并重发同一批次 | Bob 以 6 赞进入第 100 名，两个标签页均标出自己。Alice 掉榜后显示自己的 5 赞，重发前后总数均为 9,911 |
| Alice 再点 1 次，与 Bob 同为 6 赞 | 数值 ID 较小的 Alice 排第 100，Bob 两个标签页只显示自己的 6 赞 |
| 对一名观众丢掉一条榜单增量，再制造一次名次变化 | 前端发现基础版本缺失，自动请求 HTTP 快照 |
| Alice 再点 95 次，从第 100 名升至第一名 | 实际以 99 个 32 字符用户名制造大范围换位。编码差量超过 8 KiB，观众收到刷新通知并取回完整榜。Alice 以 101 赞排第一 |
| 查询其他用户个人数、跨房间查询、无令牌查询 | 附加 user_id 参数返回 400，错配场次返回 409，未认证返回 401 |
| 刷新 Bob 的页面，结束并重新开播 | 刷新后仍显示本人 6 赞。旧场记录了 105 个唯一批次、10,007 个赞。新场打开为空榜和 0 赞，之后点两下排第一。另一个房间始终空榜 |

自动测试还覆盖 101 人倒序进入时的同分排序、超过 2⁵³ 的相邻点击数与最大 uint64 用户 ID、迟到个人消息、旧 HTTP 响应以及换场时取消请求。以上为功能与一致性验证，未测量吞吐量或大场次补建耗时。

## 复测入口

先在独立、全新的 Compose 项目创建 16 分区的 live.events，并对该项目的 MySQL 执行 `go run ./cmd/migrate`。使用 `LIVESTREAM_TEST_MYSQL_DSN`、`LIVESTREAM_TEST_REDIS` 和 `LIVESTREAM_TEST_KAFKA` 指向该环境。双消费组用例会创建用户和场次并消费测试 Topic，重复执行全套应使用新的测试环境。

在 `backend/` 实际运行了以下命令。启用三个测试服务的竞态检查执行 15 个顶层用例及旧场补建子用例，无跳过或竞态报告。标为 `[no test files]` 的包只有编译检查。

```sh
go vet ./...
go test ./...
go test -race -v -count=1 -timeout 120s ./...
go build ./cmd/api ./cmd/worker ./cmd/migrate ./cmd/backfill-like-board
```

在 `frontend/` 执行 `npm run type-check`、`npm test`、`npm run build`，2 个前端自动化用例通过。仓库中可直接复测 [Redis 排榜及补建](../../../backend/internal/messaging/leaderboard_integration_test.go)、[真实双消费组](../../../backend/internal/messaging/dual_consumer_integration_test.go)、[推送边界与个人分发](../../../backend/internal/realtime/leaderboard_test.go)和[前端补缺与换场](../../../frontend/tests/leaderboard.test.mjs)。浏览器步骤使用本轮临时 Playwright 驱动，表中保留观察值。

消费组和连接管理沿用项目对参考实现 ISSUE-04、ISSUE-05 的约束。补建只查询 Kafka 管理接口，房间推送在连接管理锁外读取 Redis 与公开用户名。[参考项目审查](../../reviews/live-stream-danmaku-review.md)

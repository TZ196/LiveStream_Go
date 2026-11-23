# 实时点赞锁等待与 Kafka 分区交接

2026 年 11 月 23 日在 WSL2、Go 1.26.8、franz-go 1.20.4、Kafka 4.0.0 单 Broker 和 Redis 7.4.7 上验证。业务代码基于 `86f4e74`。本轮向独立 Compose 工程发送每批 5 个赞的消息。测试复用生产的消费循环、Redis Lua 和房间总数队列，以一个旧成员和一个加入的成员隔离交接。场次预先初始化，观众用有界发送通道表示。[锁实验](../../../backend/internal/realtime/like_lock_test.go) · [交接实验](../../../backend/internal/realtime/like_lock_integration_test.go)

## 成员与执行顺序

在日常开发 Broker 上只读查询 `kafka-consumer-groups.sh --bootstrap-server localhost:29092 --all-groups --describe --members --verbose`。API 的实时组、独立 Worker 进程的持久化组各有 4 个成员。两个组各自覆盖全部 16 个分区，每个成员分到 4 个分区。每个成员在一个 goroutine 中执行 `PollRecords(ctx, 1)`、处理、同步提交，再取下一条。业务回调的并发上限分别为 4。每组人数可用 `LIVESTREAM_REALTIME_CONSUMERS` 和 `LIVESTREAM_PERSISTENCE_CONSUMERS` 独立调整。[消费组](../../../backend/internal/messaging/consumer_group.go) · [消费循环](../../../backend/internal/messaging/consumer.go)

实时路径依次校验场次、调用 Redis Lua、取得 `manager.mu` 并记录待发的房间快照。处理返回成功后，消费者使用同一个 10 秒 Context 同步提交 Kafka 位移，然后调用 `AllowRebalance()`。等待锁的时间计入这 10 秒。房间总数由独立的 200 毫秒定时器推送。消费者使用 `BlockRebalanceOnPoll()` 和库默认的 60 秒 `RebalanceTimeout`。[实时处理](../../../backend/cmd/api/likes.go) · [房间队列](../../../backend/internal/realtime/like_updates.go) · [franz-go 配置](https://github.com/twmb/franz-go/blob/v1.20.4/pkg/kgo/config.go)

## 实测

纯锁对照中，A 同时持有两把独立的锁 20 秒，两个 B 共用一个 10 秒 Context。两轮普通 `sync.Mutex` 都等待了 20.010 秒，拿到锁时 Context 已过期。测试里的可取消锁使用容量为 1 的令牌通道，等待者分别在 10.009 和 10.003 秒收到 `context deadline exceeded` 并返回。

接入 Kafka 后，A 在 Redis 已经写入之后持锁。普通锁组直接占用真实的 `manager.mu`。可取消组在相同的内存更新位置插入可取消锁，原队列使用无竞争的 `manager.mu`。新成员在旧成员等锁时入组，Broker 已报告 `PreparingRebalance`。下表列出首轮与复测两个样本。处理用时从进入回调起算，交接用时从发起加入到确认新成员独占 16 个分区且目标位移已提交为止。

| 持锁条件 | 旧处理函数返回（秒） | 旧成员提交 | 交接并提交（秒） | 重放后 Redis 场次 / 个人 / 榜首 | 观众队列中的总数更新 |
| --- | --- | --- | ---: | --- | ---: |
| 普通锁，20 秒 | 20.004、20.002，成功 | 提交时 Context 已超时 | 20.542、25.534 | 5 / 5 / 5 | 1 |
| 可取消锁，20 秒 | 10.002、10.001，超时 | 处理报错，跳过提交 | 10.533、15.532 | 5 / 5 / 5 | 0 |
| 普通锁，65 秒 | 65.003、65.002，成功 | 提交时 Context 已超时 | 65.085、65.037 | 5 / 5 / 5 | 1 |

首轮两个 20 秒场景分别命中分区 9、11，消息 offset 均为 0。等待中和截止时，Redis 已保存事件指纹与分区检查点 1，Kafka 已提交位置仍为 -1（从未提交）。允许新成员处理前再次查询仍为 -1。新成员重读同一 `event_id`、分区和 offset，判重后把 Kafka 位置提交到 1。复测可取消锁时恰好命中已有数据的分区 9，当前消息 offset 为 1，旧成员退出后 Kafka 位置仍为 1，接手者重放后才提交到 2。

65 秒首轮中，分区 7 的旧处理在第 60 秒仍然等锁，Kafka 提交位置仍为 -1。Broker 从进入 `PreparingRebalance` 到移除未重新入组的旧成员用了 60.001 秒，并在随后的 1 毫秒内报告仅剩一个成员。新成员在加入后 65.066 秒读到原记录。复测时直接查询成员 ID 和分区分配，60.059 秒时新成员已独占 16 个分区，旧处理仍然等锁，到 65.002 秒才返回。旧成员超过了按时交还分区的预算，协调器能收回所有权，Go 函数则在 A 释放锁时才继续。

可取消等待的协程在截止时退出，普通锁的协程在 A 解锁后退出。交接实验关闭所有测试消费者、生产者和 Redis 客户端后，按 goroutine ID 和栈核对，均未观察到新增且存活的实验、franz-go 或 go-redis 协程，总数从 2 回到 2。范围限于这几次持锁后确实释放的实验。

## 去重、计数与在线推送

- Redis 在同一段 Lua 内检查 `event_id` 指纹，首次处理时更新场次总数、个人数、前 100 名和版本，保存指纹并推进 Redis 分区检查点。项目会在写入前校验计数上界。独立并发实验向 Redis 同时投影同一条 `count=100000` 记录 16 次，1 次首次计数、15 次判重，个人数和榜首都为 100000。更高 offset 的重复事件可推进检查点，旧 offset 重放不会回退，溢出用例验证计数、指纹和检查点均未部分写入。[投影脚本](../../../backend/internal/messaging/lua/project_like.lua) · [Redis 集成用例](../../../backend/internal/messaging/projection_integration_test.go) · [Redis 脚本的原子执行](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- Redis 返回首次成功后，若等待房间锁时取消，本次总数快照还未入队。新成员重放时 `QueueLikeUpdate` 遇到 `Duplicate` 直接返回，隔离观众的队列里最终有 0 条总数帧，Redis 读取仍为 5。正式 API 有暂停、重新核对和恢复时断开旧连接的逻辑，重新连接会读取当前快照。实验观测到发送队列为止，浏览器重连和 MySQL 持久化不在此次故障注入范围内。[队列处理](../../../backend/internal/realtime/like_updates.go) · [暂停与重建消费组](../../../backend/cmd/api/likes.go) · [恢复和连接快照](../../../backend/internal/realtime/manager.go)
- 若要对房间锁引入可取消等待，需要安排从 Redis 已有快照补发或重新同步，并验证观众在没有后续点赞时也能看到正确总数。恢复通知和恢复连接同样会取得 `manager.mu`，需要一并验证。对应 KAFKA-05 的实时组撤销场景、COUNT-01 的重复批次只计一次，以及参考审查 ISSUE-04、ISSUE-05。[验证计划](../plan.md) · [参考项目审查](../../reviews/live-stream-danmaku-review.md)

## 复现

在 `backend/` 已通过 `go vet ./...`、`go test -count=1 ./...`、`go test -race -count=1 ./...` 及 `go build ./cmd/api ./cmd/migrate ./cmd/worker ./cmd/backfill-like-board`。常规测试中有用例的包为 messaging、realtime 和 room，其余包输出 `no test files`。上述实验另外显式启用测试中间件并使用 `-race` 执行。

在仓库根目录用 Bash 运行。示例使用两个空闲回环端口、随机集群 ID 和单独的 Docker 卷。集成测试会在加入 `live-realtime` 之前核对 Broker 的集群 ID。Compose 解析需要两个 MySQL 环境变量，此处使用占位值，实验只启动 Kafka 和 Redis。

```bash
(
  set -eu
  repo_root=$(pwd)
  probe_dir=$(mktemp -d)
  probe_project="like-lock-$(date +%s)-$$"
  export LIVESTREAM_MYSQL_PASSWORD=unused MYSQL_ROOT_PASSWORD=unused
  export LIVESTREAM_KAFKA_PORT=39092 LIVESTREAM_REDIS_PORT=36379
  export LIVESTREAM_TEST_KAFKA="127.0.0.1:$LIVESTREAM_KAFKA_PORT"
  export LIVESTREAM_TEST_REDIS="127.0.0.1:$LIVESTREAM_REDIS_PORT"
  export LIVESTREAM_TEST_LIKE_LOCKS=1
  LIVESTREAM_TEST_LOCK_CLUSTER_ID=$(python3 -c 'import base64, uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))')
  export LIVESTREAM_TEST_LOCK_CLUSTER_ID
  cat > "$probe_dir/cluster.yaml" <<'YAML'
services:
  kafka:
    environment:
      CLUSTER_ID: ${LIVESTREAM_TEST_LOCK_CLUSTER_ID:?}
YAML
  probe_compose() {
    docker compose --env-file /dev/null -p "$probe_project" \
      -f "$repo_root/deployments/docker/compose.yaml" -f "$probe_dir/cluster.yaml" "$@"
  }
  trap 'probe_compose down -v; rm -f "$probe_dir/cluster.yaml"; rmdir "$probe_dir"' EXIT
  probe_compose up -d --wait kafka redis
  probe_compose run -T --rm --no-deps kafka-init
  cd "$repo_root/backend"
  go test -race -count=1 -v -timeout 5m \
    -run '^(TestLikeMutexDeadline|TestLikeMutexHandoff|TestProjectionIntegration)$' \
    ./internal/realtime ./internal/messaging
  probe_compose logs kafka | grep -E 'Preparing to rebalance|removed dynamic members|Stabilized group'
)
```

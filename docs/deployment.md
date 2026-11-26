# 开发与运行说明

当前使用 WSL2 Ubuntu：Go API 和 Vite 在 WSL 中运行，MySQL 由 Docker Compose 启动。每位协作者使用自己的本地 `.env` 与数据卷；共享同一个房间联调时，前端需要连接同一个 API。当前状态见 [项目状态](status.md)。

## 环境与首次配置

当前锁定 Go 1.26.8、Hertz 0.10.6、GORM 1.31.2、MySQL 适配器 1.6.0、MySQL 8.4.13；前端验证环境为 Node.js 26.13.0、npm 11.20.0。依赖以 [go.mod](../backend/go.mod)、[package.json](../frontend/package.json) 和各自锁文件为准，MySQL 镜像版本与摘要固定在 [Compose](../deployments/docker/compose.yaml)。

在 Windows 启动 Docker Desktop，为当前 WSL 发行版启用集成；在 WSL 执行 `docker version` 和 `docker compose version`，确认客户端、服务端及 Compose 可用。[Docker WSL 说明](https://docs.docker.com/desktop/features/wsl/)

以下命令从仓库根目录执行，只在缺少 `.env` 时复制样例：

```bash
if [ ! -e .env ]; then
  install -m 600 backend/configs/.env.example .env
fi
```

编辑本地 `.env`：

- 为 `LIVESTREAM_MYSQL_PASSWORD`、`MYSQL_ROOT_PASSWORD` 分别填写独立的随机密码，可分别运行 `openssl rand -hex 24` 生成。
- 为 `LIVESTREAM_JWT_SECRET` 填写 `openssl rand -base64 32` 的结果。API 要求至少 32 随机字节的标准 Base64 编码，迁移命令不读取此项。
- 示例默认 MySQL 地址为 `127.0.0.1:3306`。已有服务占用时，可统一将 `LIVESTREAM_MYSQL_ADDR` 改为 `127.0.0.1:13306`；Compose 和 Go 都读取该值。

已有 `.env` 和数据库卷时继续使用原凭据。文件保持本地私有且不提交到 Git；前端不读取服务端密钥。MySQL 只在空数据目录初始化时使用环境中的账号和密码，修改 `.env` 不会同步修改已有数据库账号。更换 JWT 密钥会使旧令牌失效。

## 启动 MySQL 和后端

从仓库根目录执行：

```bash
docker compose --env-file .env -f deployments/docker/compose.yaml config --quiet
docker compose --env-file .env -f deployments/docker/compose.yaml up -d --wait mysql
set -a
source .env
set +a
cd backend
go run ./cmd/migrate && go run ./cmd/api
```

Compose 校验只检查配置；容器健康检查执行应用账号的 `SELECT 1`，仍需单独执行迁移。迁移按顺序运行未成功的版本并记录成功，失败停止；只有迁移成功，API 才启动。当前 main 代码要求结构版本 8；本任务分支需要版本 9，升级步骤见数据设计。V7 仅新增 danmaku_messages 及索引/外键，V8 仅新增 failed_events；均不改写点赞数据。2026-11-25 已备份并将原开发库从 V6 升级至 V8，升级前后旧点赞及迁移记录一致；其他环境升级前仍须停止旧 API/Worker 并备份。旧二进制拒绝高版本，切回旧版本代码时须核对兼容。版本 6 只新增保存有效投影代次的 projection_state 表，不改写点赞事件。版本 4 建立场次、点赞及消费记录表，版本 5 为每条点赞记录增加点击数，原有记录按一次点击保留。API 启动只读检查结构，不调用 AutoMigrate。详情见[迁移与恢复边界](design/database.md#迁移与版本)。

默认 API 地址为 http://127.0.0.1:8080 。另一个终端执行：

```bash
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/readyz
```

`healthz` 预期返回 200 和 `{"status":"ok"}`；`readyz` 在数据库及版本检查通过时返回 200、`stage=P2`，数据库不可用时返回 503。需要覆盖 API 端口时，在 backend 目录执行 `go run ./cmd/api -addr 127.0.0.1:8081`。

## 启动前端与停止服务

保留后端终端，在另一个终端从仓库根目录执行：

```bash
cd frontend
npm ci
npm run dev
```

打开 http://127.0.0.1:5173 。依赖已安装且锁文件未变化时可直接运行 `npm run dev`。端口占用会报错。注册后登录，可以创建、筛选、查看房间，房主可结束直播；下次选择“我的房间”与“已结束”，进入原房间详情点击“重新开播”。

登录凭证保存于当前浏览器同源存储，刷新或重新打开页面可恢复，原有 30 分钟有效期不会延长；到期或主动退出后重新登录。同源标签页共享账号和退出状态，测试两个不同账号请使用独立浏览器或隐私窗口。首次更新旧版页面时，原内存凭证没有保存，需要登录一次。

Vite 将 `/healthz`、`/readyz`、`/api` 和 `/ws` 代理到 `LIVESTREAM_API_TARGET`，默认 http://127.0.0.1:8080 。修改目标可在 frontend 目录执行 `LIVESTREAM_API_TARGET=http://127.0.0.1:8081 npm run dev`，或按 [前端配置样例](../frontend/.env.example) 设置 `.env.local` 后重启。直播间已接入 WebSocket 点赞、本场点赞榜、AC、共享 WebSocket 弹幕与历史；完整在线人数协议仍待交付。直播页面使用项目内的静态演示封面，桌面布局为头像旁点赞数、左画面、右上榜单与右下聊天；手机纵向排列。审核拒绝只短提示，未知结果从简洁状态入口核对，不展示待发送列表或数量。

HTTP 客户端请求超时为 5 秒，离开页面取消未完成请求，写请求不自动重试。业务页面不显示开发连接检查，排查服务状态时直接访问 /healthz 和 /readyz；`npm run build` 生成的 dist 不包含开发代理，目前尚未接入 API 静态托管。

在各自终端按 Ctrl+C 退出前端和 API。需要停止数据库时，在仓库根目录执行 `docker compose --env-file .env -f deployments/docker/compose.yaml stop mysql`；该操作保留数据卷。

## 配置与资源边界

Go 程序只读取环境变量与参数，不自动加载 `.env`。支持参数的项按参数 > 环境变量 > 默认值合并，显式空值按无效配置处理。无效配置在监听前退出且不回显输入值；帮助入口为 `go run ./cmd/api -h`。

| 环境变量 | 参数 | 默认值与约束 |
| --- | --- | --- |
| LIVESTREAM_HTTP_ADDR | -addr | 127.0.0.1:8080；主机和端口，端口 1–65535 |
| LIVESTREAM_LOG_LEVEL | -log-level | info；debug、info、warn、error，不区分大小写 |
| LIVESTREAM_SHUTDOWN_TIMEOUT | -shutdown-timeout | 30s；带单位的正时长 |
| LIVESTREAM_JWT_SECRET | 无 | 无默认值；API 必填，至少 32 随机字节的 Base64 编码 |
| LIVESTREAM_MYSQL_ADDR | 无 | 127.0.0.1:3306；本地 Compose 使用回环 IP 和端口 |
| LIVESTREAM_MYSQL_DATABASE | 无 | livestream；非空 |
| LIVESTREAM_MYSQL_USER | 无 | livestream；非空，应用库账号 |
| LIVESTREAM_MYSQL_PASSWORD | 无 | 无默认值；必填，仅通过环境注入 |
| MYSQL_ROOT_PASSWORD | 无 | Compose 首次初始化使用；API 和迁移命令不读取 |

GORM 创建底层连接池，DB() 只取得同一池用于配置、检查和关闭。池最多 10 条连接、10 条空闲连接，连接复用最长 3 分钟；会话及 DATETIME 解析使用 UTC。建连、读写超时分别为 5 秒，API 启动检查共用 5 秒预算，就绪检查预算为 1 秒。禁用初始化自动 Ping 和自动版本探测，显式 PingContext，查询通过 WithContext 传递取消。以上均为保护参数，不是实测容量。

迁移建连预算为 5 秒，之后迁移阶段共用 30 秒预算；单次读写仍受 5 秒限制。迁移命名锁零等待，被占用立即失败；释放锁使用独立 2 秒预算。SIGINT/SIGTERM 可取消等待，但已发出的 DDL 可能已经生效，应核对结构后修复。API 关闭 HTTP 后再关闭连接池。

应用日志使用 slog JSON 输出到 stdout，Hertz 原生日志输出到 stderr；访问日志只记录方法、路由模板、状态、duration_ms 和 trace_id，不输出原始路径、查询、请求头或请求体。GORM 自动 SQL 日志关闭。认证参数和限流见 [HTTP 契约](design/api-spec.md)；当前只信任 TCP 来源，不能直接信任客户端转发头。

## 日常检查

后端在 backend 目录执行，当前只构建已经存在的入口：

```bash
go vet ./...
go test ./...
go build ./cmd/api ./cmd/migrate ./cmd/worker
```

修改的 Go 文件先执行 gofmt。前端在 frontend 目录执行 `npm run type-check` 和 `npm run build`。按用户要求，后端 `*_test.go` 与 frontend/tests/ 仅保留本地，不提交或推送；本机有测试文件时另外执行 `npm test`、`go test` 和适用的 `go test -race`。新检出仓库没有这些用例，`no test files` 只能表示未执行测试，不能据此宣布用例通过。集成验证须显式指定隔离的 Kafka、Redis、MySQL，历史报告中的命令依赖当时本地保留的源码。验收场景见[验证计划](verification/plan.md)。

## 后续部署阶段

P4 点赞子链路使用 Redis 7.4.7 standalone、Apache Kafka 4.0.0（KRaft 单 Broker/Controller）及 MySQL Worker；P5 接入独立弹幕链路、审核与历史，V8 补弹幕异常持久隔离；点赞保留普通重启核验及异常重建，点赞异常隔离仍待后续。Worker 只依赖 Kafka 和 MySQL，Redis 故障时仍可持久化。Redis 使用 AOF everysec、noeviction 和持久卷，恢复规则见[可靠性设计](design/reliability.md)。

P7 的 Compose 包含 API、Worker、MySQL、Redis、Kafka 五个服务。前端完成类型检查与构建后由 API 同源托管，Worker 镜像不包含前端；backend/Dockerfile 以仓库根目录为构建上下文。页面回退不能覆盖未知 `/api`、`/ws` 错误。公网 TLS、域名与生产运维不属于本阶段。

| 服务 | 宿主访问示例 | Compose 内部访问 |
| --- | --- | --- |
| API | 127.0.0.1:8080 | api-server:8080 |
| MySQL | 127.0.0.1:3306，冲突时改为 13306 | mysql:3306 |
| Redis | 127.0.0.1:16379 | redis:6379 |
| Kafka | 127.0.0.1:9092 | kafka:29092 |

Kafka 需要分别公布宿主和容器客户端地址，并配置独立 Controller 端口；只映射端口不足以保证元数据返回后的连接。V1 中间件端口默认绑定回环地址，使用独立持久卷。P7 分别验证内外地址和重启数据保留。

后续引入实时任务时，退出先停止新准入、票据和新消费，按预算等待已接受任务，只提交成功事务及连续位移，未确认发布保留 UNKNOWN。默认总退出预算仍为 30 秒；依赖健康不能替代消费者、投影及数据库的业务就绪检查。

## 本地点赞链路启动（PowerShell）

以下命令在仓库根目录执行。`.env` 为本地文件，不提交；先参考 `backend/configs/.env.example` 配置 MySQL 密码、JWT 密钥和点赞选项。`LIVESTREAM_PROJECTION_EPOCH` 须为固定的小写 UUID，首次生成后妥善保留，普通重启不得重新生成。版本 6 首次将该代次兼容写入 projection_state，成功重建后使用数据库的新代次，无需改写 .env。`LIVESTREAM_WS_ORIGIN` 必须与浏览器访问地址完全一致，例如 `http://127.0.0.1:5173`。已有点赞数据时先完成[停写和消费追平](#已有点赞数据升级到本场榜单)。从版本 4 升级须在旧进程退出后备份数据库，再执行迁移。保留 Kafka 位移和现有 Redis 投影。版本 5 可以读取既有单击事件，旧程序无法消费新批次。

```powershell
docker compose --env-file .env -f deployments/docker/compose.yaml up -d
Get-Content .env | ForEach-Object {
  if ($_ -match '^([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
    [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
  }
}
Set-Location backend
go run ./cmd/migrate
```

已有场次投影时先完成下文的排行榜补建。全新环境或补建完成后，在两个已加载相同环境变量的终端分别执行 `go run ./cmd/api` 和 `go run ./cmd/worker`；在 `frontend` 执行 `npm ci` 后运行 `npm run dev`。访问 `http://127.0.0.1:5173`，登录并进入房间点赞或发送弹幕。Kafka init 只在缺失时分别创建 live.events、live.danmaku 两个 16 分区 Topic；不能随意删除或重建已有 Topic。`/readyz` 只决定普通 API 就绪；响应 recovery 与房间 likes-state 接口独立报告点赞状态。Worker 进度单独查看。

`LIVESTREAM_REALTIME_CONSUMERS` 控制 API 内的实时组成员数，`LIVESTREAM_PERSISTENCE_CONSUMERS` 控制 Worker 内的持久化组成员数。两项默认均为 4，可分别设置为 1–16。变更后重启对应进程，Kafka 自动重新分配原有分区。可以用 Broker 自带命令核对两个组的成员和分区分配。

```sh
docker compose -f deployments/docker/compose.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --all-groups --members --verbose
```

新场次首次进入时初始化空榜，显示本场 0 赞。正常 Redis 重启自动核验并沿用完整投影；丢失或回退时自动停止后端点赞接收、保留共享连接并等待 Worker 追平，再由 MySQL 明细与去重记录构建新代次。浏览器在原场次继续暂存，恢复后自动补发。必要日志缺失、明细冲突或 Topic 改变时保持 BLOCKED 并记录原因，修复后重启 API。当前只支持单 API 与独立 Worker，不提供恢复中断的断点续建。单 Kafka Broker 与 Redis AOF everysec 均不提供任何故障下零丢失保证。

## 启用弹幕与审核

在原有私有 .env 中补齐样例新增项，两个进程使用相同 Kafka/MySQL 配置：

```dotenv
LIVESTREAM_ENABLE_DANMAKU=true
LIVESTREAM_MODERATION_WORDS=configs/moderation_words.tsv
LIVESTREAM_DANMAKU_REALTIME_CONSUMERS=4
LIVESTREAM_DANMAKU_PERSISTENCE_CONSUMERS=4
LIVESTREAM_WORKER_METRICS_ADDR=127.0.0.1:9091
```

停旧 API/Worker 并备份后，在 backend 执行迁移至当前版本 9；回到仓库根目录创建缺少的 Topic：

```bash
docker compose --env-file .env -f deployments/docker/compose.yaml run --rm kafka-init
```

保留原点赞 Topic、位移、Redis 代次和数据卷。仍运行原来的两个入口。只关闭点赞时弹幕可以独立启用；Redis/Kafka 整体不可用则相关业务都会受影响。词库为 80 条人工短语，UTF-8 TSV 的 category、term、source 三列均必填；词库加载错误只禁止新弹幕，点赞和历史按自身依赖继续服务。修改词库后重启 API，已冻结/已接受事件不重审。

Redis 进程重启或弹幕请求身份丢失时，弹幕可能进入五分钟保护。草稿和未确认请求保留；原冻结事件或 SQL 结果能证明时复用原身份，缺证据的旧请求不重新生成审核版本。此状态与点赞恢复独立，详见[可靠性设计](design/reliability.md#弹幕登记审核与结果核对)。

API /metrics 汇总两业务，Worker 默认回环 9091/metrics 报告自身持久化状态与逐分区 lag。/readyz 状态码只表示普通 API 就绪，recovery 表示点赞状态，danmaku 表示 READY/UNAVAILABLE/DISABLED，不把 Worker 或浏览器送达混入其中。

旧历史接口为 GET /api/v1/rooms/{id}/danmaku，可选 session_id、limit、cursor；活动场次的最近缓存接口为 GET /api/v1/rooms/{id}/danmaku/recent，要求 session_id，每次最多 20 条，正常查询只访问 Redis。前端列表最多 200 条，支持更早历史及事件去重，未确认请求不自动更换 ID。

## 已有点赞数据升级到本场榜单

新部署会在首次进入新场次或处理首条点赞时建立排行榜。已有场次可在升级后继续同一场直播，保留升级前已接受的点击。使用原来的数据库、Redis 实例、Kafka Topic 和 `LIVESTREAM_PROJECTION_EPOCH`。

1. 在维护窗口暂停产生新的点赞，排空已准入的请求。暂时保留旧 API 和 Worker 消费积压。从仓库根目录运行下列命令，确认 `live-realtime`、`live-persistence` 都已处理到各自 16 个分区的日志末端，然后正常退出旧 API 和 Worker。保持三个中间件运行。

   ```sh
   docker compose --env-file .env -f deployments/docker/compose.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --all-groups --describe
   ```

2. 从旧结构升级时，先停旧进程、备份并在 `backend/` 执行 `go run ./cmd/migrate` 到当前版本 9。版本 5 新增批次数，版本 6 仅保存有效代次，版本 7、8 新增弹幕与异常表，版本 9 新增可空服务端接收时间。
3. 在 `backend/` 使用新代码、原环境变量和数据库账号运行补建命令。

   ```sh
   go run ./cmd/backfill-like-board
   ```

   成功输出 `leaderboard_backfill_complete`。命令核对消费组成员、逐分区位移和投影身份，将 MySQL 唯一批次按用户求和，并与原场次总数核对后逐场切换。重复运行可得到相同累计数。若有积压，在保持入口停写的情况下使用原版本消费者追平，退出后重跑。Redis 身份、检查点或计数不一致时，保留现场并按[恢复设计](design/reliability.md)核对原因。
4. 启动新版 API、Worker 和前端，确认 `/readyz` 可用且点赞状态为 READY。在原直播间核对本场总数、前 100 名和本人赞数，再恢复入口。已写入新榜后若需要恢复旧版消费，下一次启用榜单前需重新执行本节步骤。

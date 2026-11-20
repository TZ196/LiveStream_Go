# LiveStream 开发环境与部署设计

基线 V1.1。定义环境、配置、网络和运行边界；当前实际可用情况单独记录在 [项目状态](status.md)。Compose 与 Dockerfile 在对应阶段建立。

## 环境和版本责任

代码位于当前 WSL2 Ubuntu 工作区。Go API、Worker 和 Node.js 开发服务在 WSL 内运行；浏览器与 Docker Desktop 在 Windows 侧。无需根据目录规划迁移工作区。

| 组件 | 设计基线 | 版本锁定位置及时间 |
| --- | --- | --- |
| Go、Hertz、Gorilla WebSocket | 已确认技术选择 | P1a 锁定 Go/Hertz，P3 锁定 Gorilla；backend/go.mod 与 go.sum |
| Vue 3、TypeScript、Vite、Node.js | Composition API | P1a 的 package.json、包管理锁文件和 Node 版本记录 |
| MySQL | 8.4 系列 | 当前 Compose 固定 8.4.13 和 sha256 镜像摘要 |
| Redis | 7 系列，standalone | P4 Compose 固定具体版本及内存预算 |
| Kafka | Apache Kafka，KRaft 单 Broker 与 Controller | P5 选择兼容版本并固定镜像；记录客户端与 Broker 组合 |
| MySQL、Redis、Kafka 的 Go 客户端 | 满足事务、Lua、幂等生产与手动提交语义 | 当前使用 GORM v1.31.2 + gorm.io/driver/mysql v1.6.0，底层 go-sql-driver/mysql v1.10.2；Redis/Kafka 在接入阶段锁定 |

机器上的工具版本不自动成为已经验证的依赖组合。P1a 已在 backend/go.mod 固定 Go 基线和 Hertz 版本，前端在 frontend/package.json 与 package-lock.json 锁定依赖；各自的构建、联调结果见项目状态。后续协议和中间件接入仍需独立验证。

## Docker Desktop 与 WSL

在 Windows 启动 Docker Desktop，确认使用 WSL2 引擎，并在 Settings → Resources → WSL Integration 启用当前发行版。随后在本工作区验证 Docker 客户端、服务端和 Compose 均可使用；单有 docker 可执行文件不代表容器引擎已接通。[Docker 官方说明](https://docs.docker.com/desktop/features/wsl/)

进入 P1b 前需确认 WSL 内 Docker 客户端、服务端及 Compose 均可使用，实际复查结果统一记录在项目状态。连通性通过后，仍需在 P1b 验证固定版本的 MySQL 镜像、容器启动和 Migration。

## 分阶段依赖

P1a 仅启动 API 与前端开发服务；P1b 至 P3 使用 MySQL；P4 增加 Redis；P5 增加 Kafka 与 API 实时消费者；P6 增加 Worker。Compose 可以在同一文件中逐步增加服务，不要求首期启动全部中间件。

Worker 仅依赖 Kafka 和 MySQL，不依赖 Redis，保证投影故障时仍能完成日志持久化和恢复屏障。API 根据当前阶段检查必要能力，不因尚未交付的 Worker 不存在而启动失败。

## 网络地址

| 服务 | WSL 混合开发访问 | Compose 内部访问 |
| --- | --- | --- |
| API | 127.0.0.1:8080 | api-server:8080 |
| 前端开发服务 | 127.0.0.1:5173 | P7 不运行 Vite 开发服务 |
| MySQL | 127.0.0.1:3306 | mysql:3306 |
| Redis | 127.0.0.1:6379 | redis:6379 |
| Kafka 客户端 | 127.0.0.1:9092 | kafka:29092 |

Kafka 配置 EXTERNAL 和 INTERNAL 两个客户端 listener，分别向宿主机客户端公布 127.0.0.1:9092、向容器客户端公布 kafka:29092；Controller 使用独立内部端口。仅映射 9092 而未配置 advertised.listeners，不能保证客户端读取元数据后继续连接。[Kafka advertised.listeners](https://kafka.apache.org/44/configuration/broker-configs/#advertised.listeners)

端口冲突时使用配置覆盖并同步文档，不把容器里的 localhost 误当作另一个服务。外部端口默认绑定回环地址；V1 的明文中间件访问仅适用于本机开发网络。

## 配置和秘密

backend/configs 保存无秘密样例，部署环境负责数据库凭据、JWT 密钥和服务地址注入；启动时验证必填配置，不使用已知默认密码或默认签名密钥。

当前已接入以下配置。支持命令行的项按命令行参数 > 环境变量 > 默认值合并；MySQL 仅从环境变量读取。先合并再校验，显式空值按无效配置处理，不悄悄回退默认值。配置无效时在创建服务与监听端口前退出 1，错误只说明配置项和规则，不回显输入值。

| 环境变量 | 命令行参数 | 默认值 | 约束 |
| --- | --- | --- | --- |
| LIVESTREAM_HTTP_ADDR | -addr | 127.0.0.1:8080 | host:port，数字端口 1–65535；IPv6 使用 [::1]:8080 格式；主机解析和绑定失败在启动时报告 |
| LIVESTREAM_LOG_LEVEL | -log-level | info | debug、info、warn、error，不区分大小写 |
| LIVESTREAM_SHUTDOWN_TIMEOUT | -shutdown-timeout | 30s | 带时间单位的正时长，控制 Hertz 关闭等待预算 |
| LIVESTREAM_JWT_SECRET | 无 | 无 | API 必填，至少 32 随机字节的标准 Base64 编码；迁移命令不读取 |
| LIVESTREAM_MYSQL_ADDR | 无 | 127.0.0.1:3306 | 必须包含主机和有效端口；本地 Compose 使用回环 IP:端口 |
| LIVESTREAM_MYSQL_DATABASE | 无 | livestream | 非空；由迁移命令选择目标库 |
| LIVESTREAM_MYSQL_USER | 无 | livestream | 非空；API 使用应用账号 |
| LIVESTREAM_MYSQL_PASSWORD | 无 | 无 | 必填，仅环境变量；不放在命令行、日志或源码中 |
| MYSQL_ROOT_PASSWORD | 无 | 无 | Compose 首次初始化使用，API 和 GORM 迁移命令均不读取 |

空主机（如 :8080）会监听本机可用接口，地址族由系统决定；0.0.0.0:8080 表示所有 IPv4 接口。默认仍使用回环地址。通过 go run ./cmd/api -h 查看帮助。配置样例见 [backend/configs/.env.example](../backend/configs/.env.example)，程序只读取环境变量和参数，不自动读取 .env 文件。在 backend 目录可以用 `LIVESTREAM_HTTP_ADDR=127.0.0.1:8081 go run ./cmd/api` 覆盖地址。

P1b 在仓库根目录创建被 Git 忽略的 `.env`，权限设为 0600，填写独立的随机应用密码和 root 密码。Compose 命令显式指定 `--env-file .env`，避免从不同目录启动时加载错误文件；Go 启动前由 shell 导出变量。完整操作见 [MySQL 学习记录](learning/04-mysql-and-migration.md)。

认证阶段增加 JWT 签名密钥。本工作区已在原 .env 中单独生成并追加；新工作区可以用 `openssl rand -base64 32` 在本地生成后填入 LIVESTREAM_JWT_SECRET，不把生成值提交到 Git。API 先校验格式和解码长度，缺失或无效即退出；迁移命令仍只依赖 MySQL 配置。换密钥后旧凭证无法通过验签。前端不会读取该服务端密钥。

当前 Compose 仅包含 MySQL，数据卷为 livestream_mysql_data。容器健康检查用应用账号通过 TCP 执行 SELECT 1；健康不代表 users 表已经迁移。首次初始化的数据库名、账号和密码只对空数据目录生效：已有卷时只改 `.env` 不会修改 MySQL 中的账号，必须通过明确的数据库管理操作修改并同步配置。正常停止不删除卷。[官方镜像初始化说明](https://hub.docker.com/_/mysql)

Go 连接池最多 10 条连接、最多保留 10 条空闲连接，连接最长复用 3 分钟。建连、读写超时分别为 5 秒，整个启动检查共用 5 秒预算；就绪检查预算为 1 秒。所有会话使用 UTC，DATETIME 解析为 UTC。以上是本地开发保护参数，不是实测容量或性能成绩。HTTP 关闭后再关闭连接池；SQL 和连接取消通过 context 传递。[MySQL 驱动配置说明](https://github.com/go-sql-driver/mysql)

GORM 的 MySQL 适配器根据 DSNConfig 创建上述连接池，应用不再调用 sql.OpenDB。DB() 仅取得已存在的池，用来设置容量、PingContext 和关闭，不会再建一个池。为保留统一超时和信号取消，禁用自动 Ping；固定 MySQL 8.4 后跳过适配器不带调用方 context 的版本探测，每次结构查询使用 WithContext。当前关闭 GORM 自动 SQL 日志，错误由调用方处理，避免自动输出业务参数。[GORM 连接配置](https://gorm.io/docs/connecting_to_the_database.html)、[GORM 初始化配置](https://gorm.io/docs/gorm_config.html)

在 backend 目录运行 `go run ./cmd/migrate`，复用四个 MySQL 环境变量，不读取 HTTP 参数。目标数据库由 Compose 初始化，命令负责表结构与版本记录；当前本地应用账号已有应用库范围的建表、改表和读写权限，无需改用 root。执行成功退出 0，失败退出 1；`-h` 可在没有数据库密码时查看帮助。成功记录保存在 schema_migrations，重复运行跳过已成功版本，原有 SQL 文件不再手动执行。

迁移建连预算为 5 秒，后续迁移阶段共用 30 秒预算；单次读写仍受上述 5 秒限制，可能更早失败。同一数据库的迁移锁使用零等待：被占用立即失败。正常或失败结束均尝试释放锁，释放使用独立 2 秒预算，随后关闭连接池。SIGINT/SIGTERM 可取消等待；中断不保证已经发出的 DDL 没有生效，须核对结构后再恢复。这些预算只适用于当前本地小表，将来大表升级需要重新设计执行窗口。API/Worker 不调用 AutoMigrate。

应用生命周期和 HTTP 请求日志通过标准库 slog 写入 stdout，格式为 JSON；Hertz 框架日志保持其原生文本格式，写入 stderr，并在启动前同步日志级别。访问日志记录方法、注册路由模板、状态码、duration_ms 和业务请求的 trace_id；未匹配路由统一记为 unmatched，不记录原始路径、查询参数、请求头或请求体。duration_ms 表示服务端后续处理链耗时，不包含完整网络往返；尚未接入事件关联或性能采集系统。

认证依赖固定为 golang-jwt/jwt/v5 v5.3.1、golang.org/x/crypto v0.59.0；后者要求间接依赖 x/sys v0.50.0、x/text v0.44.0。密码与 JWT 参数、限流、请求体及业务超时以 [HTTP 契约](api-spec.md) 为准；这些是保护阈值，不是测得的吞吐能力。当前只信任 TCP 来源，部署可信反向代理时需另外设计可信代理范围，不能直接启用客户端转发头。

至少区分运行阶段、事件模式、监听地址、允许的 Origin、数据库连接、Redis 地址及预算、Kafka broker 与 Topic 身份、队列容量和超时。前端仅包含公开 API 基址，不包含后端凭据。开发代理同时支持 HTTP 与 WebSocket。

MySQL、Redis、Kafka 使用独立持久卷。停服务与删除数据卷是不同操作；正常退出不得隐含清空数据。Redis 使用 AOF everysec 和 noeviction，恢复规则仍由可靠性设计控制。

## 当前前端开发

在 frontend 目录执行 `npm ci` 安装锁定依赖，`npm run dev` 启动 127.0.0.1:5173；端口占用时报错，不悄悄切换端口。`npm run type-check` 检查 TypeScript 和 Vue 文件，`npm run build` 生成 dist，生产构建本身不代替类型检查。

浏览器以相对路径请求本站，Vite 将 /healthz、/readyz、/api、/ws 转发至 LIVESTREAM_API_TARGET，默认 http://127.0.0.1:8080。该变量只由开发配置读取，可通过进程环境或 frontend/.env.local 设置；修改后重启开发服务。Vite 自动加载 .env.local，与当前 Go 后端不自动读取 .env 的行为不同。

健康检查只在开发页面显示。它使用 5 秒请求超时和手动重试，不轮询；成功仅证明健康接口返回约定响应。账号页面现已通过同源代理接入注册、登录和当前用户查询；令牌仅存页面内存，刷新、退出或到期后重新登录。HTTP 访问统一封装，每次请求超时 5 秒，离开页面取消未完成请求；写请求不自动重试。WS 目前只有代理配置，实际协议与业务尚未接入。

生产构建不包含上述开发代理；前端静态资源与 API 的同源托管仍在 P7 实现。

## P7 容器交付

API Server、Worker、mysql、redis、kafka 为五个服务。前端先完成类型检查及生产构建，产物复制到 API 镜像中由 API 同源提供静态资源；Worker 镜像不需要前端产物。backend/Dockerfile 使用仓库根目录作为构建上下文，以获取 frontend 和 backend 输入。

静态页面回退只适用于前端页面路径，未知 /api 和 /ws 请求保持接口错误，不能返回 index.html。本地浏览器访问 API 发布端口即可使用前端。公网 TLS、域名和生产运维不属于此阶段。

## 启动与关闭

依赖健康不等于业务就绪。API 在完成配置校验、数据库迁移版本检查、当前模式的消费者及投影状态检查后进入相应能力就绪状态；Worker 校验 Topic 身份和 checkpoint 后开始消费。

收到退出信号后停止新准入，关闭票据签发，向连接发送退出原因；停止消费新批次，等待已接受的有限任务完成或达到退出期限；只提交成功事务及连续处理位置。仍未确认的发布保留 UNKNOWN，不自动改为失败。资源关闭顺序覆盖生产者、消费者、数据库与 Redis 连接池，默认总退出预算为 30 秒。

日志记录进程版本、阶段及配置摘要但不包含秘密。P7 必须分别验证宿主机客户端和容器客户端的 Kafka 生产消费，以及持久卷保留后的重启恢复。

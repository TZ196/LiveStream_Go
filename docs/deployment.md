# LiveStream 开发环境与部署设计

基线 V1.1。定义环境、配置、网络和运行边界；当前实际可用情况单独记录在 [项目状态](status.md)。本轮不安装软件、不生成 Compose 或 Dockerfile。

## 环境和版本责任

代码位于当前 WSL2 Ubuntu 工作区。Go API、Worker 和 Node.js 开发服务在 WSL 内运行；浏览器与 Docker Desktop 在 Windows 侧。无需根据目录规划迁移工作区。

| 组件 | 设计基线 | 版本锁定位置及时间 |
| --- | --- | --- |
| Go、Hertz、Gorilla WebSocket | 已确认技术选择 | P1a 锁定 Go/Hertz，P3 锁定 Gorilla；backend/go.mod 与 go.sum |
| Vue 3、TypeScript、Vite、Node.js | Composition API | P1a 的 package.json、包管理锁文件和 Node 版本记录 |
| MySQL | 8.4 系列 | P1b Compose 使用具体补丁版本或镜像 digest |
| Redis | 7 系列，standalone | P4 Compose 固定具体版本及内存预算 |
| Kafka | Apache Kafka，KRaft 单 Broker 与 Controller | P5 选择兼容版本并固定镜像；记录客户端与 Broker 组合 |
| MySQL、Redis、Kafka 的 Go 客户端 | 满足事务、Lua、幂等生产与手动提交语义 | 各接入阶段评估一次并锁定，禁止使用未固定的 latest |

当前机器上的 Go 与 Node 版本是环境观察值，不自动成为已经验证的依赖组合。初始化时先验证最小构建与协议接入，再将版本写入锁文件。

## Docker Desktop 与 WSL

在 Windows 启动 Docker Desktop，确认使用 WSL2 引擎，并在 Settings → Resources → WSL Integration 启用当前发行版。随后在本工作区验证 Docker 客户端、服务端和 Compose 均可使用；单有 docker 可执行文件不代表容器引擎已接通。[Docker 官方说明](https://docs.docker.com/desktop/features/wsl/)

本轮检查已发现当前 WSL 的 Docker/Compose 不可用。该事项属于进入 P1b 的环境准备工作，不阻塞继续完成设计，也不能记录为已经修复。

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

至少区分运行阶段、事件模式、监听地址、允许的 Origin、数据库连接、Redis 地址及预算、Kafka broker 与 Topic 身份、队列容量和超时。前端仅包含公开 API 基址，不包含后端凭据。开发代理同时支持 HTTP 与 WebSocket。

MySQL、Redis、Kafka 使用独立持久卷。停服务与删除数据卷是不同操作；正常退出不得隐含清空数据。Redis 使用 AOF everysec 和 noeviction，恢复规则仍由可靠性设计控制。

## P7 容器交付

API Server、Worker、mysql、redis、kafka 为五个服务。前端先完成类型检查及生产构建，产物复制到 API 镜像中由 API 同源提供静态资源；Worker 镜像不需要前端产物。backend/Dockerfile 使用仓库根目录作为构建上下文，以获取 frontend 和 backend 输入。

静态页面回退只适用于前端页面路径，未知 /api 和 /ws 请求保持接口错误，不能返回 index.html。本地浏览器访问 API 发布端口即可使用前端。公网 TLS、域名和生产运维不属于此阶段。

## 启动与关闭

依赖健康不等于业务就绪。API 在完成配置校验、数据库迁移版本检查、当前模式的消费者及投影状态检查后进入相应能力就绪状态；Worker 校验 Topic 身份和 checkpoint 后开始消费。

收到退出信号后停止新准入，关闭票据签发，向连接发送退出原因；停止消费新批次，等待已接受的有限任务完成或达到退出期限；只提交成功事务及连续处理位置。仍未确认的发布保留 UNKNOWN，不自动改为失败。资源关闭顺序覆盖生产者、消费者、数据库与 Redis 连接池，默认总退出预算为 30 秒。

日志记录进程版本、阶段及配置摘要但不包含秘密。P7 必须分别验证宿主机客户端和容器客户端的 Kafka 生产消费，以及持久卷保留后的重启恢复。

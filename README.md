# LiveStream 实时直播互动系统

面向本地开发的直播互动项目。当前已实现注册登录、刷新恢复登录、直播间管理，以及基于独立直播场次的 WebSocket 批量点赞、个人数、前百排行榜和异步持久化。完成边界与待开发能力见[项目状态](docs/status.md)。

后端使用 Go、CloudWeGo Hertz、GORM、MySQL、Redis 和 Kafka，前端使用 Vue 3、TypeScript 和 Vite。API 内的实时消费组和独立 Worker 的持久化组分别处理同一条点赞事件。

## 开始使用

按 [开发与运行说明](docs/deployment.md) 配置本地 `.env`，通过 Compose 启动 MySQL、Redis、Kafka 和 Topic 初始化，执行独立迁移，再分别运行 API、Worker 和前端。每位协作者使用自己的本地配置与数据卷。

前端默认地址为 http://127.0.0.1:5173 ，API 为 http://127.0.0.1:8080 。登录后可以创建和管理直播间；刷新保持登录，到期或主动退出后重新登录。已有房间可在“我的房间”中筛选“已结束”，进入详情后重新开播。

## 项目入口

| 需要了解的内容 | 入口 |
| --- | --- |
| 当前完成情况、待解决问题 | [项目状态](docs/status.md) |
| 后端目录、模块依赖与点赞链路 | [Backend 结构](backend/ARCHITECTURE.md) |
| 接下来的开发顺序 | [开发计划](docs/development-plan.md) |
| 需求、业务规则与技术设计 | [文档导航](docs/README.md) |
| 验收案例与实际结果 | [验证计划](docs/verification/plan.md)、[阶段报告](docs/README.md#验证记录) |
| 协作、代码和 Git 规则 | [AGENTS.md](AGENTS.md) |

后端代码位于 `backend/`，前端位于 `frontend/`，Compose 配置位于 `deployments/docker/`。常驻后端入口为 `cmd/api` 和 `cmd/worker`，维护命令为 `cmd/migrate` 和 `cmd/backfill-like-board`。

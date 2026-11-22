# LiveStream 实时直播互动系统

面向本地开发的直播互动项目。当前已实现注册登录、当前用户查询，以及直播间创建、筛选分页、详情和房主关闭；下一步开发 WebSocket 连接基础能力与 Redis 点赞。

后端使用 Go、CloudWeGo Hertz、GORM 和 MySQL，前端使用 Vue 3、TypeScript 和 Vite。弹幕、在线连接统计、Kafka 与 Worker 持久化按阶段接入；当前没有音视频播放、礼物或支付功能。

## 开始使用

按 [开发与运行说明](docs/deployment.md) 配置本地 `.env`，启动 MySQL，执行迁移，再分别运行 API 和前端。当前 Compose 只包含 MySQL；每位协作者使用自己的本地配置与数据卷。

前端默认地址为 http://127.0.0.1:5173 ，API 为 http://127.0.0.1:8080 。登录后可以创建和管理直播间；登录凭证保存在页面内存，刷新、退出或到期后需要重新登录。

## 项目入口

| 需要了解的内容 | 入口 |
| --- | --- |
| 当前完成情况、待解决问题 | [项目状态](docs/status.md) |
| 接下来的开发顺序 | [开发计划](docs/development-plan.md) |
| 需求、业务规则与技术设计 | [文档导航](docs/README.md) |
| 验收案例与实际结果 | [验证计划](docs/verification/plan.md)、[阶段报告](docs/README.md#验证记录) |
| 协作、代码和 Git 规则 | [AGENTS.md](AGENTS.md) |

后端代码位于 `backend/`，前端位于 `frontend/`，Compose 配置位于 `deployments/docker/`。当前后端入口是 `cmd/api` 和独立迁移命令 `cmd/migrate`，Worker 在对应阶段建立。

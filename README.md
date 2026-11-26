# LiveStream 实时直播互动系统

一个面向学习和本地开发的直播间互动项目：用户可以注册登录、管理直播间，在独立直播场次中发送弹幕和连续点赞。项目侧重实时连接、消息处理、数据持久化与故障恢复，不包含音视频推流。

## 已实现的功能

- **用户与房间**：注册、登录、JWT 认证和登录状态恢复；创建、查询、结束和重新开播房间，每次开播使用独立场次。
- **WebSocket 连接**：通过 HTTP 换取一次性房间票据，再建立共享连接；支持心跳、断线重连和慢连接保护。
- **弹幕与审核**：发送文本弹幕，使用 AC 自动机和本地词库审核；通过审核后写入 Kafka，再由实时消费者向同场观众广播。
- **点赞与榜单**：前端合并连续点击，展示本场点赞总数、前 100 名及当前用户的累计点赞数。
- **事件持久化与历史**：点赞和弹幕由独立 Worker 消费并写入 MySQL；直播页从 Redis 查询最近五分钟的弹幕，支持分页和缓存重建，另保留 MySQL 历史查询接口。
- **重试与恢复**：请求使用稳定标识，处理重复消费和结果未确认；点赞投影支持恢复，弹幕永久异常事件可隔离，具体边界见[可靠性设计](docs/design/reliability.md)。

## 技术栈

| 部分 | 技术 | 用途 |
| --- | --- | --- |
| 后端 | Go、CloudWeGo Hertz | HTTP API、中间件和业务服务 |
| 实时连接 | Gorilla WebSocket、Hertz HTTP Adaptor | WebSocket 握手和房间消息收发 |
| 数据访问 | GORM、MySQL 8.4 | 用户、房间、事件和消费检查点 |
| 实时数据 | Redis 7、Lua | 点赞投影、弹幕请求状态、限流和最近弹幕缓存 |
| 消息队列 | Apache Kafka | 缓冲事件，供实时、持久化和缓存消费者独立处理 |
| 前端 | Vue 3、TypeScript、Vite | 页面、状态管理和交互 |
| 本地环境 | Docker Compose | 启动 MySQL、Redis 和 Kafka 等中间件 |

依赖的具体版本以 [backend/go.mod](backend/go.mod)、[frontend/package.json](frontend/package.json) 和 [Compose 配置](deployments/docker/compose.yaml)为准。

## 核心链路

```text
浏览器 → HTTP API → 用户与房间管理 → MySQL
    └→ WebSocket → Kafka
                     ├→ API 实时消费者 → 房间内连接
                     ├→ Worker → MySQL 持久化
                     └→ 弹幕缓存消费者 → Redis 最近缓存
```

点赞和弹幕使用各自的 Topic 与消费组；图中的最近缓存消费者只处理弹幕。点赞实时消费者会更新 Redis 投影，再推送总数和榜单。Kafka 的接受确认不表示所有观众都已收到消息，也不表示数据库已经落盘。

## 本地运行

按照[开发与运行说明](docs/deployment.md)准备本地配置，使用 Compose 启动中间件和初始化 Topic，执行独立数据库迁移，再分别启动 API、Worker 和前端。当前代码要求数据库结构版本 V9；已有数据库升级前请先核对运行说明中的兼容和备份步骤。

前端默认地址为 http://127.0.0.1:5173，API 默认地址为 http://127.0.0.1:8080。每位协作者使用自己的本地配置与数据卷。

## 项目结构

```text
backend/
  cmd/api/          API 服务入口
  cmd/worker/       异步持久化入口
  cmd/migrate/      独立数据库迁移入口
  internal/         用户、房间、点赞、弹幕及实时连接实现
  migrations/       数据库版本迁移
frontend/           Vue 前端
deployments/docker/ 本地中间件 Compose 配置
docs/               设计、运行说明和验证记录
```

## 了解设计与当前进度

- [文档导航与学习路线](docs/README.md)：按功能设计顺序阅读。
- [后端源码导航](backend/ARCHITECTURE.md)：从请求入口追踪点赞、弹幕和恢复实现。
- [项目状态](docs/status.md)：查看已完成、待解决和下一步。
- [验证计划与报告](docs/verification/plan.md)：区分验收目标和实际测试结果。

当前以单个 API Server 为基线；在线人数展示、多 API 实例广播、完整故障覆盖和性能容量结论尚未交付。其他限制见[项目状态](docs/status.md)。

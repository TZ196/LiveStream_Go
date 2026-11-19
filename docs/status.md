# LiveStream 项目状态

更新日期：2026 年 11 月 19 日。D0 需求与设计基线已完成；P1a 工程骨架进行中，首步最小 Hertz HTTP 服务已实现并验证，完整 P1a 尚未完成。

## 已完成

- 将技术基线统一为 V1.1，建立需求、领域、架构、HTTP、WebSocket、Kafka、数据、可靠性、环境、计划和验证文档。
- 明确公开房间、连续点赞、关闭不重开、在线连接计数与无离线必达的范围。
- 完成一次性票据、稳定请求身份、结果未知、持久化事务、异常隔离及停写恢复设计。
- 建立需求到模块责任和验收案例的映射，并同步 AGENTS 文档入口和分阶段构建规则。
- 按用户要求删除旧 Word 工作副本、修订稿和归档目录，仅维护当前 Markdown 基线。
- 将每轮修改检查后提交并推送当前远程分支的持续授权写入 AGENTS。
- 已通过 [PR #1](https://github.com/TZ196/LiveStream_Go/pull/1) 将 V1.1 文档基线及任务分支、PR 工作流合并到 main，并完成本地同步。
- 复查 WSL 内 Docker 客户端、服务端及 Compose 均可访问，之前的 Docker 连通性阻塞已解除。
- 建立场景、设计取舍、边界、失败处理与工程证据的阶段交付要求，补充指标口径和项目讲解示例；业务场景目前仍为设计与验证计划。
- 建立 backend/go.mod、go.sum、cmd/api 与 internal/httpserver，使用 Go 1.26.8 和 Hertz v0.10.6；尚未建立 Worker 入口。
- 实现 GET /healthz、监听地址参数、启动错误非零退出和信号关闭，补充接口测试及 [首步学习记录](learning/01-minimal-http-server.md)。

当前可运行范围为本地 HTTP 服务与存活探针；用户、房间、实时互动、数据库、业务容器和恢复流程均未实现。

## 实际环境观察

| 项目 | 实际检查结果及日期 |
| --- | --- |
| 当前工作区 | /home/tz196/projects/LiveStream_Go，WSL2 Ubuntu |
| Go 与 Hertz | 11 月 19 日：go1.26.8 linux/amd64，Hertz v0.10.6；本步构建与检查通过 |
| Node.js 与 npm | 11 月 18 日：v26.13.0、11.20.0 可执行；前端依赖组合尚未验证 |
| Docker 与 Compose | 11 月 18 日：客户端及 Engine 29.5.3、Docker Desktop 4.78.0；Compose v5.1.4，WSL 可连接服务端 |
| 后端、前端、Compose | 11 月 19 日：已有 backend/go.mod 和 API 入口；尚无 frontend/package.json、deployments/docker/compose.yaml |
| Git 基线与流程 | 已有 PR #1 至 #3 合并到 main；本步任务分支为 feat/p1a-health-server，沿用提交、推送、PR 检查及合并流程 |
| CI 与分支保护 | 尚未配置 CI 或 main 强制检查；当前检查由任务执行，工程建立后再接入自动检查 |

Go/Hertz 仅验证了当前 HTTP 骨架；后续 WebSocket 和数据组件仍需分别验证。Docker 连通性不代表业务镜像已拉取、容器已启动或 Migration 已验证。

## 验证记录

| 检查 | 状态 | 说明 |
| --- | --- | --- |
| 文档内容、契约一致性与链接 | 通过 | 11 月 19 日：核对健康响应、P1a 部分完成状态与学习步骤；20 份 Markdown、69 个本地链接无缺失目标；git diff --check 通过 |
| 场景论证与指标定义 | 通过 | 11 月 18 日已核对 7 个场景及指标口径；本轮为 P1a 首步补充实际实现和 BASE-01 证据，其余业务结果待验证 |
| JSON 及身份算法示例 | 通过 | 11 月 18 日检查 7 个 JSON 示例及 Kafka 身份算法；未修改这些原有示例 |
| Go 格式与静态检查 | 通过 | 11 月 19 日：gofmt 检查无输出，backend 内 go vet ./... 成功 |
| 后端测试、race、构建 | 通过 | 11 月 19 日：go test -v ./...、go test -race ./...、go build -o bin/api ./cmd/api 成功；测试覆盖健康响应、405 和 404，cmd/api 无单元测试 |
| 依赖校验 | 通过 | 11 月 19 日：go mod verify 返回 all modules verified；已审查 go.mod/go.sum 与导入；首次 tidy 下载遇到网络 EOF，使用单次命令代理重试后成功，未修改全局 Go 配置 |
| BASE-01 真实进程检查 | 通过 | 11 月 19 日：本地随机端口 45689、46861，分别验证 GET 200、JSON 与 no-store、POST 405、未知路径 404、同端口第二进程退出 1；SIGTERM 与 SIGINT 均退出 0 |
| 开发工具与 Docker 连通性 | 通过 | 11 月 18 日：go version、node --version、npm --version、docker version、docker compose version 均成功；本轮未重测 Docker |
| 前端类型检查与构建 | 未运行 | 尚无工程 |
| Compose 启动、故障及恢复案例 | 未运行 | 尚无 Compose 配置和业务实现；目前仅验证 Docker 服务端连通性 |
| 性能测试 | 未运行 | 当前仅做功能检查，没有吞吐、延迟或连接容量实测结果 |

## 首步复盘与证据边界

场景为启动本地 API 并确认它能响应，对应 BASE-01。启动入口与 HTTP 接入分开，健康检查不访问未来中间件；本步没有业务规则，因此不建立空 Service 或 Repository。

首次进程检查发现默认 Netpoll 在端口占用时 panic，退出码为 2；改用 Hertz 内置标准网络实现后，监听错误进入启动入口并退出为 1，复测通过。该选择及替代方案见学习记录；没有测量两种网络实现的性能差异。

接口测试不监听真实端口，进程检查另外覆盖启动和退出。race 只覆盖现有测试路径；当前信号检查没有长连接、后台任务或中间件需要排空，不能作为未来业务退出正确性的证据。30 秒是配置的退出预算，不是实测耗时。

## 下一小步

继续 P1a 的配置校验、日志和就绪检查，再建立 Vue/TypeScript/Vite 页面与 HTTP 封装。完整 P1a 仍以前端调用健康接口、后端构建测试及前端类型检查和构建作为退出条件。

P1b 接入 MySQL 时验证固定版本镜像、启动和 Migration；Redis/Kafka 的客户端、镜像与容量预算按接入阶段核验。每个小任务完成适用检查和讲解后提交推送，任务完成后按已确认的 PR 工作流交付。

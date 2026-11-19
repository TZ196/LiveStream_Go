# LiveStream 实时直播互动系统

LiveStream 面向本地开发，目标包含用户认证、直播间管理、实时弹幕、点赞、在线连接统计和异步事件持久化。当前进入 P1a 工程骨架，首个实现为 Hertz HTTP 服务及存活探针。

设计基线为 **V1.1，2026 年 11 月 18 日**。后端采用 Go、CloudWeGo Hertz 和 Gorilla WebSocket，前端采用 Vue 3、TypeScript 和 Vite；MySQL、Redis、Kafka 随开发阶段接入。

从 [文档导航与职责](docs/README.md) 开始阅读。需求见 [需求规格](docs/requirements.md)，实施顺序见 [开发计划](docs/development-plan.md)，实际完成情况见 [项目状态](docs/status.md)。工程协作规则见 [AGENTS.md](AGENTS.md)。

## 运行第一步后端

当前使用 Go 1.26.8，Hertz 固定为 v0.10.6。在仓库根目录执行：

```bash
cd backend
go run ./cmd/api
```

在另一个终端执行 `curl -i http://127.0.0.1:8080/healthz`，预期得到 HTTP 200 和 `{"status":"ok"}`。端口可通过 `go run ./cmd/api -addr 127.0.0.1:8081` 覆盖。

文件职责、请求过程和验证步骤见 [第一步学习记录](docs/learning/01-minimal-http-server.md)。目前尚未实现业务接口、就绪检查、前端或中间件接入，P1a 仍在进行中。

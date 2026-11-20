# 第一步：运行最小 Hertz 后端

日期：2026 年 11 月 19 日。属于 P1a 工程骨架的第一小步，目标是理解并跑通一次 HTTP 请求。后续仍需完成配置、日志与就绪检查、前端工程和联调，才能验收完整 P1a。

本页说明首步概念与操作；后续配置见 [第二步学习记录](02-config-logging-readiness.md)。当前已进入 P1b，启动后端前还需要 MySQL 和环境变量，完整运行命令以 [仓库 README](../../README.md) 为准；健康接口的含义保持不变。

## 场景与范围

开始用户认证、房间和实时互动之前，先需要一个能够接收 HTTP 请求的后端进程。本步提供 GET /healthz，用于判断进程能否响应；它不查询数据库、Redis 或 Kafka，也不证明任何业务功能已经可用。具体响应定义见 [HTTP 契约](../api-spec.md)。

选择独立的存活探针，是为了将“进程是否响应”和“必要依赖是否就绪”分开。依赖故障时，进程本身仍可能正常；后续 /readyz 会按当前阶段检查依赖。当前还没有数据库访问，因此健康 Handler 不需要额外的 Service 或 Repository。

## 文件职责与阅读顺序

| 文件 | 职责与需要理解的内容 |
| --- | --- |
| [go.mod](../../backend/go.mod) | 定义后端 Module 路径、Go 基线和依赖版本；本步直接依赖 Hertz v0.10.6 |
| [go.sum](../../backend/go.sum) | 保存依赖内容的校验值；它不是全部依赖版本的锁文件，也不能证明依赖不存在安全问题 |
| [main.go](../../backend/cmd/api/main.go) | 可执行程序入口：读取监听参数、组装服务、等待退出信号，并将启动错误转换为非零退出码 |
| [server.go](../../backend/internal/httpserver/server.go) | 创建 Hertz 实例，注册路由，定义健康接口响应；New 本身不监听端口 |

一个 Module 可以包含多个 Package 和可执行入口。backend/internal/httpserver 是后端内部包，cmd/api 是本步的可执行入口；后续 Worker 仍可以使用同一个 Module。Module 主要管理依赖，不代表微服务数量。依赖命令与校验方式参考 [Go 官方依赖管理说明](https://go.dev/doc/modules/managing-dependencies)。

先沿着 main → run → httpserver.New → 路由注册 → health 阅读正常请求路径，再回头看启动失败和退出信号处理。

## 启动与发起请求

在仓库根目录打开第一个终端：

```bash
cd backend
go run ./cmd/api
```

默认监听 127.0.0.1:8080。go run 会编译并启动程序；看到服务监听日志后，在另一个终端请求：

```bash
curl -i http://127.0.0.1:8080/healthz
```

预期得到 HTTP 200、JSON Content-Type、Cache-Control: no-store，以及以下响应体：

```json
{"status":"ok"}
```

请求处理过程是：curl 连接监听端口 → Hertz 根据方法 GET 和路径 /healthz 匹配路由 → health 使用请求上下文写入响应头和 JSON → 客户端读取响应。

health 的 context.Context 参数与 Hertz RequestContext 是两个不同概念：前者用于取消、期限及跨调用传递信息，后者提供当前 HTTP 请求与响应的操作接口。本步健康处理没有下游调用，所以没有使用前者；后续业务层也不会接收 Hertz RequestContext。

监听地址可以通过本步的命令行参数覆盖：

```bash
go run ./cmd/api -addr 127.0.0.1:8081
```

当前环境变量、配置校验与日志见第二步学习记录。默认绑定回环地址，仅用于本地开发。

## 验证正常和失败路径

| 操作 | 预期结果与意义 |
| --- | --- |
| GET /healthz，不携带身份令牌 | 200 和约定 JSON；存活探针可以独立使用 |
| POST /healthz | 405；方法也是路由匹配的一部分 |
| GET /does-not-exist | 404；未知路径不能误返回健康结果 |
| 保持一个实例运行，再启动同端口的第二个实例 | 第二个实例启动失败并以非零状态退出；不能把端口冲突当作正常退出 |
| 对构建后的进程发送 SIGINT 或 SIGTERM | Hertz 执行关闭流程，正常结束；配置的最长等待为 30 秒，这是退出预算，不是实测耗时 |

可以手动验证路由边界：

```bash
curl -i -X POST http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/does-not-exist
```

main 通过缓冲为 1 的信号通道接收退出通知。自定义 signal waiter 选择“服务出错”或“收到退出信号”：前者保存错误，最终退出码为 1；后者交给 Hertz 进行正常关闭。这样做的原因是 Hertz Spin 不直接返回 error，需要显式保留启动错误。相关生命周期见 [Hertz 关闭说明](https://www.cloudwego.io/docs/hertz/tutorials/basic-feature/graceful-shutdown/)。

实际验证发现，在当前 Linux 环境中，Hertz v0.10.6 默认 Netpoll 实现遇到端口占用会直接 panic。因此本步显式使用 Hertz 内置的标准网络实现，它会将监听错误返回给启动入口。仍使用 Hertz 的路由和请求上下文。保留默认 Netpoll 也是可选方案，但需要另行处理其启动失败行为；当前没有负载测量，不能声称一种网络实现比另一种更快。配置方式见 [Hertz 网络库说明](https://www.cloudwego.io/docs/hertz/tutorials/basic-feature/network-lib/)。

以下检查由助手执行，日常学习不要求逐条重复。在 backend 目录执行：

```bash
go vet ./...
go build -o bin/api ./cmd/api
```

构建后，可在 backend 目录按以下顺序检查真实进程。先确认 18080 端口空闲；第一条命令启动的进程 PID 只用于本次检查：

```bash
./bin/api -addr 127.0.0.1:18080 &
api_pid=$!
curl -i --retry 5 --retry-connrefused --retry-delay 1 http://127.0.0.1:18080/healthz
./bin/api -addr 127.0.0.1:18080
printf '第二个进程退出码：%s\n' "$?"
kill -TERM "$api_pid"
wait "$api_pid"
printf '第一个进程退出码：%s\n' "$?"
```

第二个进程应退出为 1，第一个收到 SIGTERM 后应退出为 0。重新运行一次，将 kill -TERM 改为 kill -INT，可检查 SIGINT。这里使用构建后的二进制，避免把 go run 的编译包装进程和实际服务进程混淆。信号测试只覆盖当前无业务任务的退出，不能证明未来连接和后台任务已正确排空。

当前已按用户要求删除接口测试文件，健康响应、路由边界、监听、端口冲突和信号退出通过实际进程检查。`go test ./...` 会显示 `no test files`，不能据此声称接口用例通过。实际检查结果记录在 [项目状态](../status.md)。

## 本步复盘

你应能解释：Go Module 与 Package 的区别；为什么 main 只负责组装和生命周期；路由为何同时匹配方法和路径；存活与业务就绪的区别；为什么启动失败需要非零退出码。

本步证据是构建、接口与进程行为验证。尚未进行吞吐或延迟压测，不能从几个成功请求推导连接容量或性能成绩。继续阅读 [第二步](02-config-logging-readiness.md) 时，先理解配置数据和读取顺序，再逐个学习日志与就绪状态。

# 第二步：配置、日志与就绪状态

日期：2026 年 11 月 19 日。继续 P1a，承接首步 HTTP 服务。本步建立可以切换地址和日志级别的运行入口，以及独立的就绪状态；前端与业务仍在后续阶段。

阶段提示：本文保留 P1a 的讲解背景。当前 P1b 已增加 MySQL 配置和依赖检查，运行命令以 README 为准；空 dependencies 与三项配置只描述当时的实现，新增部分见 [第四步学习记录](04-mysql-and-migration.md)。

当前代码已按教学要求整理：展开配置和日志的嵌套写法，关键步骤使用中文注释，删除配置与 HTTP 测试文件。先按下面的顺序理解运行代码，每次只学习一个部分。

## 场景与取舍

本地调试和后续容器部署需要不同的监听地址，排查问题时也需要切换日志级别。把这些值集中为配置，先校验再启动，可以在运行前发现输入错误。当前只使用三个实际需要的配置项，具体默认值、优先级和约束统一维护在 [部署设计](../deployment.md)。

配置只使用标准库 flag、os.LookupEnv、net 与 time；当前参数少，不引入 YAML、Viper 或热更新。命令行覆盖环境变量，环境变量覆盖默认值；先合并再校验，允许正确的命令行参数覆盖环境中的错误值。显式空值不能被当作未配置。以后配置规模增加时再评估文件配置与更复杂的管理工具。

应用日志使用标准库 slog，按字段输出 JSON，便于按状态码和路由查找请求。Hertz 框架日志继续使用其原生 stderr 文本输出，启动前同步级别；因此当前不是全进程统一 JSON 日志。没有引入新的第三方依赖。

## 代码阅读顺序

| 文件 | 本步职责 |
| --- | --- |
| [config.go](../../backend/internal/config/config.go) | 定义 Config；合并默认值、环境和参数，检查格式与范围，失败不返回可用配置 |
| [main.go](../../backend/cmd/api/main.go) | 读取配置、创建日志、组装 HTTP 服务；初始化完成后标记 ready，退出前标记 stopping |
| [logging.go](../../backend/internal/logging/logging.go) | 创建应用 JSON Logger，启动前配置 Hertz 日志级别 |
| [server.go](../../backend/internal/httpserver/server.go) | 用具名字段保存 Hertz、日志和状态，注册探针；New 组装服务，入口再调用 Hertz.Spin |
| [access_log.go](../../backend/internal/httpserver/access_log.go) | Server 的请求日志方法；先执行后续 Handler，再记录方法、路由模板、状态码和处理耗时 |
| [配置样例](../../backend/configs/.env.example) | 展示环境变量名称与安全默认值；程序不会自动加载该文件 |

配置模块不导入 Hertz；日志实例显式传给 HTTP 模块。main 仍只组织启动过程，业务规则以后进入 Service。HTTP 请求上下文留在接入层。

## 先读懂配置文件

`Config` 是配置的数据类型，`Default()` 返回默认配置，`cfg` 是保存某次配置结果的变量名。`Usage` 只保存 `-h` 显示的中文帮助文字，实际默认值写在 `Default()` 中。

`Load(args)` 按五段顺序执行，代码中有对应中文注释：

1. 从 `Default()` 取得默认值，放进地址、日志级别文字和退出时间文字三个变量。
2. 直接调用 `os.LookupEnv` 读取环境变量；存在时覆盖对应变量。
3. 用 `flags.StringVar` 将命令行参数绑定到这些变量，再调用 `flags.Parse(args)` 更新值。
4. 检查合并后的地址、级别和时长，把有效文字转换成对应类型。
5. 创建 `Config` 并返回；入口取得配置后再组装 HTTP 服务。

以监听地址为例，下面是 Load 中的局部代码：

```go
envAddr, exists := os.LookupEnv("LIVESTREAM_HTTP_ADDR")
if exists {
    httpAddr = envAddr
}

flags.StringVar(&httpAddr, "addr", httpAddr, "HTTP 监听地址")
```

`StringVar` 用 `&httpAddr` 取得变量的位置，解析命令行时可以直接更新它。后续代码读取 `httpAddr` 就能取得最终地址，不再使用返回指针后反复解引用的写法，也不传入额外的环境查询函数。

## 配置怎样生效

在 backend 目录，原来的启动方式和 -addr 参数继续可用：

```bash
go run ./cmd/api
```

也可以在启动命令前设置环境变量：

```bash
LIVESTREAM_HTTP_ADDR=127.0.0.1:8081 LIVESTREAM_LOG_LEVEL=debug go run ./cmd/api
```

同一项同时出现时，命令行参数生效。例如下面的最终监听地址是 127.0.0.1:8082：

```bash
LIVESTREAM_HTTP_ADDR=127.0.0.1:8081 go run ./cmd/api -addr 127.0.0.1:8082
```

入口调用 `config.Load(os.Args[1:])`；`os.Args` 的首项是程序名，所以从第二项开始交给参数解析器。Load 直接读取当前进程的环境变量，只返回配置，不建立网络连接。

端口 0、缺少端口、未知日志级别、零或负数退出时长等输入会在监听前报错，进程退出码为 1。配置只做格式与范围检查；地址能否解析、端口是否可绑定，仍由启动阶段确认。错误说明配置项，不回显原始值。

## 存活和就绪的职责

/healthz 表示进程还能处理探针。/readyz 表示当前阶段的初始化是否完成，以及是否正在退出。响应字段与状态码见 [HTTP 契约](../api-spec.md)，这里不另定义一份协议。

Server 默认处于 starting；main 完成当前初始化后调用 MarkReady。收到信号或运行错误后调用 MarkStopping，再进入框架关闭流程。状态通过 atomic.Int32 读取与更新，探针请求和停止流程不会直接并发读写一个普通变量。

MarkReady 使用 CompareAndSwap，只允许 starting 变为 ready。若停止流程先将状态改为 stopping，迟到的 MarkReady 不会重新开放就绪状态；这是一个单向生命周期，而不是任意来回切换的开关。

P1a 的必要外部依赖集合为空，所以 dependencies 是空对象，恢复状态为 not_applicable。当前在监听前完成初始化，进程外通常先遇到连接不可用，再看到 ready；停止时也可能直接连接失败，不保证外部探针一定观察到 503。本轮保留原子状态转换逻辑，但删除了相应自动化测试，未重新执行进程内 503 和并发转换用例。后续接入 MySQL 后，必须把真实初始化与依赖条件加入判断，不能继续无条件标记 ready。

## 日志边界

main 的 api_starting 记录表示准备启动，不能当作监听已成功的证据；监听错误会进入 api_failed。应用访问日志记录注册路由模板，例如 /healthz，而不记录原始 URL、查询参数、请求头或请求体；未匹配路径统一记录为 unmatched，避免把未知路径中的用户输入写入日志。

main 先创建默认级别的启动日志，便于报告配置错误；配置有效后才创建正式日志。`logging.New` 依次建立选项、JSON Handler 和 Logger。`accessLog` 是 Server 的普通方法，直接使用保存的日志实例；其中 `c.Next(ctx)` 执行后续请求处理，返回后再读取状态和耗时。

duration_ms 从访问日志中间件进入到后续处理返回，包含这段服务端处理时间，不是浏览器端的请求往返时间，也不是压测结论。业务 trace_id、request_id 和 event_id 等关联信息随相应业务建立。

Hertz Spin 目前不会把 Shutdown 阶段的错误返回给入口，只在框架日志记录；api_stopped 仅表示 Spin 返回。本阶段没有长连接或后台业务任务，这些行为不能证明后续业务已经完整排空。

## 实施检查与下一步

本步由助手执行必要检查，不要求用户逐条重复。当前保留五个运行 Go 文件，`config_test.go` 和 `server_test.go` 已删除，也没有搬到其他目录。配置优先级、无效输入、帮助、HTTP 响应、日志边界和信号退出通过真实进程抽查，具体覆盖和限制见 [项目状态](../status.md)。

基础检查为 backend 目录下的 `go vet ./...` 和 `go build -o bin/api ./cmd/api`；`go test ./...` 当前仅报告 `no test files`，没有执行用例。场景编号仍为 BASE-02，不把编译成功或旧测试结果当成本轮完整行为覆盖。

后续的 Vue 前端与 HTTP 封装已建立，见 [第三步学习记录](03-frontend-and-health.md)。讲解时仍按配置、入口、日志、HTTP、浏览器请求的顺序逐步理解。完整 P1a 的退出条件按开发计划执行，实际检查见项目状态。当前修改先供讲解与用户审阅，确认交付后再提交、推送和合并。

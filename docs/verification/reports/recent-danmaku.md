# 最近弹幕缓存验证

日期：2026-11-25。测试使用本机 Docker 中新建的临时 Redis 7.4.7、MySQL 8.4.13 和 Kafka 4.0.0 容器，与原开发库隔离；Go 工具链 1.26.8、Windows amd64。未使用现有开发数据。

- 通过：`go vet ./...`、`go test ./...`，API、migrate、worker 三个入口构建；前端 `type-check`、`build`。前端 `npm test` 报告 0 个用例。
- 通过：独立 Redis 集成测试验证 45 条按 20、20、5 分页，相同毫秒、分页中新增数据、重复写入、场次隔离、重复写入不刷新 TTL 和旧锁不能覆盖缓存。
- 通过：隔离 Kafka + MySQL + Redis 联合测试：真实迁移至 V8，Kafka 接受事件后先不推进 MySQL 检查点；确认恢复等待、恢复锁阻止并发缓存写入；写入数据库及检查点后完成恢复，验证重复写入幂等、独立缓存消费组提交位点。
- 本地验证命令：在 `backend/` 设置 `LIVESTREAM_TEST_REDIS_ADDR`、`LIVESTREAM_TEST_MYSQL_DSN`、`LIVESTREAM_TEST_KAFKA_ADDR` 指向隔离测试服务后运行 `go test -v ./internal/danmaku -run TestRecent -count=1`。测试会执行真实迁移和插入数据，仅可指向临时空库与空 Topic。测试源码按仓库当前 `.gitignore` 保留本地，不被 Git 跟踪。
- 未覆盖：浏览器真实滚动、Redis 进程重启及实际丢失、多个请求并发触发恢复、MySQL 长时间积压或故障、缓存组崩溃与日志过期、消费者重平衡、跨实例恢复、连接级发送压力。实际容量、热点与恢复耗时没有测量。
- 时间口径限制：事件时间来自客户端，且允许未来时钟偏差；恢复时可能重新加载此前经钳制后已过期的记录，不能保证严格按服务端接受时间保留五分钟。
- 通过：使用临时 Go 1.26.8 Linux 容器运行 `go test -race ./internal/danmaku/... ./internal/realtime/...`。容器未配置中间件地址，因此跨服务集成测试跳过；`realtime` 无测试文件。Windows Go 环境 `CGO_ENABLED=0` 不能直接运行 `-race`。

## 2026-11-26 服务端接收时间

任务分支追加 V9 和弹幕 V2：首次冻结时保存服务端接收时间，旧 V1 的接收时间保持未知。隔离 MySQL 8.4.13、Redis 7.4.7、Kafka 4.0.0 中执行 `go test -v ./internal/danmaku -run 'TestRecent|TestLegacy' -count=1`：客户端时钟快四分钟时，重建返回服务端时间；旧事件解码和持久化对象转换、分页、去重、锁屏障和缓存组位点检查通过。测试源码按仓库规则保留本地。

首次测试暴露恢复用 Kafka 订阅客户端可能加入缓存组并抢占分区，已改为仅元数据客户端，隔离环境复测通过。尚未覆盖旧 V8 库原地升级、真实客户端重复请求的服务端时间冻结、机器时钟跳变及跨服务故障演练。V1 事件不进入 V2 最近缓存；API 与 Redis 的系统时钟需要同步。

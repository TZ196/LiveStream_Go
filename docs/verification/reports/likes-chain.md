# 点赞链路阶段验证

2026-11-22，Windows 本机，Go 1.26.8；单 Kafka 4.0.0、Redis 7、MySQL 8.4。除明确标明协作环境外，故障与破坏性样例均使用单独建立的临时容器及测试库。

| 项目 | 结果 | 范围 |
| --- | --- | --- |
| Go 格式、`go vet ./...`、`go test ./...`、API/Worker/迁移命令构建、`go mod verify` | 通过 | 无测试文件的包仅编译，无业务用例 |
| 前端 `npm run type-check`、`npm run build` | 通过 | Vue 前端 |
| Compose 配置和中间件 | 通过 | 协作环境 MySQL 8.4.13、Redis 7.4.7 和 Kafka 4.0.0 健康；`live.events` 16 分区、单副本，初始化容器成功退出 |
| 场次迁移 | 通过 | 独立空库版本 1→4；关闭、重复关闭、重开和活动场次唯一约束通过；协作库由 3 升至 4，迁移前已备份，旧 OPEN 房间保留并建立开始时间未知的当前场次 |
| 事件重复 | 通过 | 独立 Kafka 同一请求发送两次，两个消费组各自处理；Redis 点赞及 revision 各为 1，MySQL interaction_events 一条，检查点在事务后推进 |
| 浏览器点赞 | 通过 | 隔离环境与协作环境均实际注册测试账号、创建房间、点击按钮；首次点击后页面、Redis、MySQL 均为 1；协作 API 重启后继续点击，同场均为 2 |
| 重开独立场次 | 通过 | 隔离浏览器关闭再开播，各点一次；MySQL 两个 session 各为 1 |
| Redis 暂停 | 通过 | 隔离 Redis 暂停 14 秒期间 MySQL 消费仍完成；实时消费者超时重连，恢复后页面显示 1 |
| Worker 停止 | 通过 | 隔离 Worker 停止时实时计数更新，MySQL 为 0；Worker 重新启动后积压写入 MySQL，为 1 |
| Redis 投影缺失保护 | 通过 | 仅在独立 Redis 删除测试场次投影，API 重启拒绝启动并报告缺失；未验证完整重建 |
| race | 未执行 | 本机没有启用 CGO 和 C 编译器，`go test -race` 不能运行 |
| 完整恢复和容量 | 未完成 | Redis 全量重建、异常消息持久隔离、长时间宕机超过日志保留期、Kafka 确认丢失注入、多实例广播、k6 负载及极端并发均未验证 |

接受 ACK 仅表示 Broker 已确认；Redis 和 MySQL 异步完成，不能把 ACK 解释为两者已经完成。Kafka 单副本、Redis AOF everysec 和日志保留期限均存在数据丢失边界；客户端时间来自 UUIDv7，不是可信服务端发生时间。

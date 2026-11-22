# LiveStream 当前状态

更新日期：2026 年 11 月 22 日。认证和房间管理已合入 main；业务基线为 `5f021b1`，原两个功能分支已删除。运行方式见 [部署说明](deployment.md)。

## 已完成与未完成

| 范围 | 当前状态 |
| --- | --- |
| P1a 工程骨架 | Hertz API、配置日志、健康/就绪接口、Vue 前端 |
| P1b 用户认证 | MySQL、GORM、独立迁移、注册登录、Bearer 鉴权、当前用户查询及页面 |
| P2 房间管理 | 创建、筛选分页、详情、房主关闭；迁移版本 3；重复关闭保留首次时间 |
| P3/P4 实时互动 | 票据、WebSocket、RoomHub、弹幕、点赞、在线连接数、Redis 尚未实现 |
| P5/P6 消息与持久化 | Kafka、Worker、历史事件及投影恢复尚未实现 |
| P7/P8 交付与测量 | 当前 Compose 仅含 MySQL；完整应用容器部署和容量压测尚未完成 |

## 验证与环境

11 月 22 日合并前，Go 格式、静态检查、API/migrate 构建及前端类型检查、构建通过。`go test` 成功退出，但 10 个包均无测试文件，没有执行用例。详细结果见 [P2 报告](verification/reports/p2.md#11-月-22-日合并复查)。

同日 WSL 的 Docker 命令提示集成不可用；使用已有 Windows Docker CLI 完成了 Compose 文本校验，未重新启动服务验证。当前服务是否运行需按部署说明现场检查。

历史业务验证见 [P1a](verification/reports/p1a.md)、[P1b](verification/reports/p1b.md)、[P2](verification/reports/p2.md)。部分临时脚本和原始结果已不存在，报告标注了证据边界；本轮文档整理不作为业务复测。

## 设计更新

点赞事件拟按 `room_id:event_id` 分散至 16 分区，弹幕仍按房间分区保序；Kafka 与相关验证尚未实现。单节点 Redis、Broker 和广播仍须测量，关闭后最终完成屏障仍待设计。详见 [Kafka 契约](design/kafka-events.md)。

## 待解决问题

| 编号 | 问题 | 后续落实与验收 |
| --- | --- | --- |
| TODO-01 | 房主关闭房间时仍有弹幕或点赞在途 | 按 [领域规则](design/domain-model.md#关闭与在途请求) 落实准入与关闭顺序、在途结果和停止广播；P3/P4 验证 ROOM-02 发送并发，P5 复测 Kafka 路径 |
| TODO-02 | 断网、关页、心跳超时或进程退出后清理连接和计数 | RoomHub 落实单次清理、读写循环退出、定时器回收及重连；P3 验证 ROOM-03、WS-04、WS-05、UI-01 |

已有数据库条件关闭只解决重复关闭；上述实时并发问题仍未解决。

## 下一步

1. 完成点赞所需的票据、WebSocket 和 RoomHub 连接基础能力，同时落实连接清理与房间关闭边界。
2. 接入 Redis，原子完成点赞请求接受、去重和计数，并推送前端状态。
3. 完整弹幕、Kafka 与 Worker 按 [开发计划](development-plan.md) 继续推进；P4 不宣称具备 P6 的历史恢复能力。

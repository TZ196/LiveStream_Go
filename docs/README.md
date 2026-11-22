# LiveStream 文档导航

当前维护 V1.1 设计基线。设计说明目标行为，完成情况以 [项目状态](status.md) 为准，实际检查结果按阶段保存在验证报告中。

## 从哪里开始

| 目的 | 阅读入口 |
| --- | --- |
| 在本机运行项目 | [开发与运行说明](deployment.md) |
| 查看已完成、待解决和下一步 | [项目状态](status.md) |
| 按阶段继续开发 | [开发计划](development-plan.md) |
| 设计功能或对接接口 | 下方设计索引 |
| 检查功能与查看证据 | [验证计划](verification/plan.md)及下方阶段报告 |
| 查参考项目的来源与问题 | [源码审查](reviews/live-stream-danmaku-review.md) |

## 设计索引

| 文档 | 唯一维护的内容 |
| --- | --- |
| [需求规格](design/requirements.md) | 业务范围、需求编号与完成条件 |
| [领域模型](design/domain-model.md) | 权限、房间状态、点赞和在线连接数口径 |
| [系统架构](design/architecture.md) | 进程、模块分工、依赖与资源边界 |
| [HTTP 接口](design/api-spec.md) | HTTP 请求响应、鉴权、分页与错误码 |
| [WebSocket 协议](design/websocket-protocol.md) | 握手、消息字段、心跳与连接生命周期 |
| [Kafka 事件](design/kafka-events.md) | 事件格式、分区、消费组与位移提交 |
| [数据设计](design/database.md) | MySQL 表、约束、索引、Redis 键与迁移 |
| [可靠性设计](design/reliability.md) | 幂等、接受确认、故障分类与计数恢复 |

## 验证记录

[验证计划](verification/plan.md)定义验收方法；以下报告记录实际执行过的检查，均标注日期、范围和证据可访问性：

- [P1a：工程骨架](verification/reports/p1a.md)
- [P1b：数据库与用户认证](verification/reports/p1b.md)
- [P2：房间管理及主干合并复查](verification/reports/p2.md)

## 维护约定

每项规则只在对应设计中维护，其余文档通过链接引用。重要选择与边界直接写入对应设计；验证报告保存实际命令、环境、结果和限制。当前状态页只保留摘要，不累计历史交付日志。

学习记录、独立决策文档和项目复盘讲解文档已按用户要求删除，不另建同类副本。移动文件时同步 [工程规则](../AGENTS.md) 和引用路径；业务或协议发生变化时同步设计及验收案例。

# LiveStream 文档导航

当前维护 V1.1 设计基线。设计说明目标行为，完成情况以 [项目状态](status.md) 为准，实际检查结果按阶段保存在验证报告中。

## 如果你是从头学习这个项目

建议按一次业务操作的路径阅读，而不是一次读完所有协议字段：

1. 先看[项目状态](status.md)，知道当前实现到哪里；再看[系统架构](design/architecture.md)，区分 API、Worker 和三种中间件。
2. 看[领域模型](design/domain-model.md)，弄清房间与一次开播场次的区别，再读[数据设计](design/database.md)中的业务表和消费表。
3. 打开[后端源码导航](../backend/ARCHITECTURE.md)，沿“账号与房间 → 连接 → 点赞 → 弹幕”逐条从入口读到实现。遇到字段或错误码，再查 [HTTP](design/api-spec.md)、[WebSocket](design/websocket-protocol.md) 和 [Kafka](design/kafka-events.md) 专文。
4. 最后读[可靠性设计](design/reliability.md)，理解重试、重复消费及恢复；[验证报告](#验证记录)用来区分设计、已有实现与曾经测过的行为。

每份设计只维护自己的定义。单实例在线人数与切换已经实现，当前部署范围见 [项目状态](status.md)。

## 从哪里开始

| 目的 | 阅读入口 |
| --- | --- |
| 在本机运行项目 | [开发与运行说明](deployment.md) |
| 查看已完成、待解决和下一步 | [项目状态](status.md) |
| 按阶段继续开发 | [开发计划](development-plan.md) |
| 从源码读懂后端结构及点赞/弹幕链路 | [Backend 结构](../backend/ARCHITECTURE.md) |
| 准备项目面试 | [项目面试简介](interview-project.md) |
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
- [Kafka 点赞 Producer 基础验证](verification/reports/kafka-producer.md)
- [点赞链路阶段验证](verification/reports/likes-chain.md)
- [Backend 精简重构与点赞回归](verification/reports/backend-refactor.md)
- [点赞 Redis 正常重启与异常恢复](verification/reports/like-recovery.md)
- [弹幕、AC 审核与双业务隔离](verification/reports/danmaku.md)
- [最近弹幕缓存与服务端时间](verification/reports/recent-danmaku.md)

## 维护约定

每项规则只在对应设计中维护，其余文档通过链接引用。重要选择与边界直接写入对应设计；验证报告保存实际命令、环境、结果和限制。当前状态页只保留摘要，不累计历史交付日志。

学习记录、独立决策文档和项目复盘讲解文档已按用户要求删除，不另建同类副本。移动文件时同步 [工程规则](../AGENTS.md) 和引用路径；业务或协议发生变化时同步设计及验收案例。

# LiveStream 当前状态

更新于 2026 年 11 月 24 日。原点赞恢复分支已合入并推送 main，提交 c800921。弹幕在 feat/danmaku-ac 上继续，尚未合入 main。[运行说明](deployment.md) · [源码入口](../backend/ARCHITECTURE.md)

## 当前进度

| 范围 | 状态 |
| --- | --- |
| 用户与房间 | 注册登录、JWT、刷新恢复、创建/筛选/详情、结束与重新开播 |
| 原点赞 | 保留 500 毫秒批次、200 毫秒总数、前 100 名及本人累计、独立两组；正常重启核验及异常全量恢复沿用原实现 |
| 本轮弹幕 | 独立 Topic/两组、80 条词库、AC 发布前审核、Redis 原子限流/请求状态、冻结版本重试、场次串行分发、事务持久化、历史游标 |
| 共享连接与前端 | 页面一条连接，两业务能力/队列分开；点赞恢复保留连接；弹幕有限重试、审核反馈、历史去重和重连补查 |
| 数据版本 | 弹幕分支要求 V7，V1–V6 未改写；仅迁移隔离测试库，原开发库未升级 |
| 部署与测量 | 中间件 Compose、两个宿主入口；已做真实浏览器及短时负载，应用镜像和长时容量仍待后续 |

## 验证入口

本轮见[弹幕、AC 和业务隔离报告](verification/reports/danmaku.md)：真实三中间件、四组独立消费、双 Worker 事务故障、共享连接、浏览器、短时 30 连接/120 弹幕，以及原点赞 AOF 回退/Redis 清空/Worker 追平回归。

历史依据：[点赞恢复](verification/reports/like-recovery.md)、[精简重构](verification/reports/backend-refactor.md)、[初版点赞](verification/reports/likes-chain.md)、[P1a](verification/reports/p1a.md)、[P1b](verification/reports/p1b.md)、[P2](verification/reports/p2.md)。历史报告不作为本轮复测证据。

## 待解决问题

| 编号 | 问题与下一步 |
| --- | --- |
| TODO-01 | 旧场消息不进入新场已验证；关房在途操作最终完成屏障、旧点赞长期结果查询仍待设计 |
| TODO-02 | 队列及退出已有约束与 race 检查；完整在线人数、连接标识、自定义关闭通知、全局连接配额未交付 |
| TODO-03 | failed_events 持久隔离、保留期/磁盘阈值自动停写及公网长时负载未交付 |
| TODO-04 | 弹幕请求身份变化采用五分钟保守保护；无原冻结记录且无 SQL 结果的旧请求可能长期未确认，不提供 Outbox 保证 |

## 下一步

审阅弹幕分支并决定是否合并；合并与部署仍需本次明确授权。继续按[开发计划](development-plan.md)处理在线状态、异常隔离及完整部署，优化以测量为依据。

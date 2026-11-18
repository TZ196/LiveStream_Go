# WebSocket 一次性票据鉴权决策

日期：2026 年 11 月 18 日。状态：纳入 V1.1 设计，P3 实现。

## 背景

浏览器 WebSocket 不能像 HTTP 客户端一样任意设置 Authorization 请求头，而系统需要在升级之前校验身份及房间权限。

## 决策

HTTP 使用 30 分钟 Bearer JWT。浏览器通过已认证 HTTP 获取绑定用户、房间、Origin 和令牌期限的一次性随机票据，最长 30 秒，然后在 WebSocket URL 中只传该短时票据。服务端在升级前原子消费票据。

单 API 的票据放在有界内存中，不让 Redis 成为 P3 的前置依赖。原始票据不进入日志，API 重启后重新申请。连接到 JWT 原始期限即关闭；V1 不实现刷新令牌。

## 替代方式与取舍

HttpOnly Cookie 可用于同源认证，但会同时引入 Cookie 生命周期和跨站请求约束；直接在 URL 放长期 JWT 会扩大日志暴露面；升级后首帧认证需要额外管理未认证连接。本版选择短票据，以维持握手前验证并兼容浏览器。

代价是握手前多一次 HTTP 请求，并需要一次性消费与容量控制。对应测试为 WS-01、WS-02。详细字段以 [HTTP](../api-spec.md) 和 [WebSocket](../websocket-protocol.md) 契约为准。

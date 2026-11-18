# LiveStream HTTP 接口契约

基线 V1.1。负责 HTTP 字段、认证、错误和分页。业务口径见 [领域模型](domain-model.md)，长连接消息见 [WebSocket 协议](websocket-protocol.md)。

## 通用约定

路径前缀为 /api/v1，JSON 使用 UTF-8。user_id、room_id、记录 id、like_count 和递增版本号均为十进制字符串，避免 JavaScript 整数精度丢失；ID 为无前导零的正整数，计数和版本可为 "0"。online_count、limit 和持续秒数为整数。HTTP 时间为 UTC RFC 3339，固定毫秒精度。

成功响应为 {"data": ... , "trace_id": "..."}。错误响应为 {"error": {"code": "...", "message": "...", "retryable": false}, "trace_id": "..."}。trace_id 是诊断标识，不是业务幂等键。输入不接受未定义字段。

除注册、登录、健康和就绪接口外，请求携带 Authorization: Bearer 访问令牌。Content-Type 不匹配返回 415，格式错误返回 400，权限不足不能通过客户端传递用户 ID 绕过。

## 认证和凭据

访问令牌为 JWT，算法固定 HS256，校验签名、iss=livestream-api、aud=livestream-browser、sub、iat 和 exp，拒绝客户端选择其他算法。sub 为用户 ID 字符串；有效期 30 分钟，允许最多 30 秒时钟偏差。签名密钥使用至少 32 随机字节，通过环境注入，不提供可运行的默认密钥。

前端将令牌保存在内存，刷新页面后重新登录；V1 不实现刷新令牌。退出登录清空内存并关闭该页面连接，不宣称已经撤销其他客户端持有的 JWT。过期长连接按 WebSocket 协议关闭。

密码使用 Argon2id，初始参数 m=19456 KiB、t=2、p=1，每条密码独立 16 字节随机盐和 32 字节输出，保存带参数的编码结果；参数可在性能验证后提高。此起点参考 [OWASP 密码存储指南](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)。登录失败统一返回 INVALID_CREDENTIALS，日志不保存密码、令牌或原始票据。

注册与登录在 P1b 即采用有界的进程内限流，每个来源 IP 每 60 秒合计最多 10 次尝试，最多并行执行 4 个密码计算；达到边界返回 429。P4 起可将窗口计数迁入 Redis，避免将 Redis 提前设为登录前置依赖。

## 接口清单

| 方法与路径 | 请求 | 成功 data 与状态 | 说明 |
| --- | --- | --- | --- |
| POST /api/v1/auth/register | username、password | User，201 | 用户名规范化后唯一；不自动登录 |
| POST /api/v1/auth/login | username、password | access_token、token_type=Bearer、expires_in=1800、User 字段 user，200 | 无效凭据统一 401 |
| POST /api/v1/rooms | title | Room，201 | owner_id 来自令牌 |
| GET /api/v1/rooms | status、owner、limit、cursor | items: Room[]、next_cursor，200 | status 默认 OPEN，可为 CLOSED；owner 可省略或为 me；按 id 倒序 |
| GET /api/v1/rooms/{id} | 无 | Room，200 | 包含关闭房间 |
| POST /api/v1/rooms/{id}/close | 空 JSON 对象 | Room，200 | 仅房主；重复关闭返回相同状态及 closed_at |
| POST /api/v1/rooms/{id}/ws-ticket | 空 JSON 对象 | ticket、expires_at、ws_path，201 | 仅开放房间；规则见下文 |
| GET /api/v1/rooms/{id}/danmaku | limit、cursor | items: Danmaku[]、next_cursor、consistency=eventual，200 | P6 起；已落库数据按 id 倒序 |
| GET /api/v1/rooms/{id}/stats | 无 | RoomStats，200 | 返回统计可用性，不将未知值伪装为零 |

User 字段：id、username、created_at。Room 字段：id、owner_id、title、status、created_at、closed_at；closed_at 在 OPEN 时为 null。Danmaku 字段：id、event_id、room_id、user_id、username、content、created_at；username 是发送时快照，created_at 是事件时间。

创建房间未提供 HTTP 幂等键，前端禁止重复点击和自动重发。结果未知时先刷新本人房间列表进行确认，不能将网络超时直接解释为创建失败。弹幕与点赞的幂等协议仅适用于 WebSocket 业务请求。

## 票据接口

Bearer 认证通过、Origin 合法且房间开放时，签发 32 随机字节的 base64url 票据。有效期为 30 秒且不超过 JWT 剩余有效期；exp 已到时不利用时钟宽限继续签发票据。只在当前 API 内存保存票据摘要、用户及用户名快照、房间、签发 Origin 和令牌到期时间。

响应不缓存，设置 Cache-Control: no-store。ws_path 形如 /ws/rooms/1001?ticket=短时票据，前端根据当前环境补充 ws 或 wss 地址。原始票据只用于一次握手，握手失败后重新申请。不能将 access_token 放入此 URL。

票据数量上限见 [架构容量边界](architecture.md)；超限拒绝申请，不无限缓存。API 重启会使未使用票据失效，客户端重新申请即可。浏览器 WebSocket 构造函数没有自定义请求头参数，因此采用 HTTP 换票方式，见 [MDN API](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/WebSocket)。

## 统计响应

```json
{
  "data": {
    "room_id": "1001",
    "online_count": 2,
    "like_count": "12",
    "likes_available": true,
    "projection_epoch": "b3fca53d-31d9-48ce-9054-289ab36cc18f",
    "likes_revision": "12",
    "server_instance_id": "0fbc0e03-c11d-45a8-a09e-fc92555dac48",
    "presence_revision": "4",
    "as_of": "2026-11-18T08:00:00.000Z"
  },
  "trace_id": "http-example"
}
```

RoomStats 的以上字段均存在。P3 尚无点赞或投影恢复期间，like_count、projection_epoch、likes_revision 为 null，likes_available=false，在线数仍可返回；前端显示“统计暂不可用”。已初始化的零计数返回字符串 "0" 和有效投影版本。

## 分页

默认 limit=20，允许 1 至 100；超过范围返回 INVALID_ARGUMENT。cursor 为服务端生成的不透明 base64url 字符串，包含上页末尾 id 及绑定的过滤条件；解码后逐项校验，不直接拼接 SQL。筛选条件改变时旧游标返回 INVALID_CURSOR。

弹幕以数据库 id 作为稳定分页位置，表示持久化顺序，不能据此推断跨客户端发送的绝对先后。next_cursor 无下一页时为 null，空页为 items=[]。实时流与历史页通过 event_id 合并。

## HTTP 错误

| 状态 | code | 可重试条件 |
| --- | --- | --- |
| 400 | INVALID_ARGUMENT、INVALID_CURSOR | 修改输入后发新请求 |
| 401 | UNAUTHORIZED、TOKEN_EXPIRED、INVALID_CREDENTIALS | 重新认证 |
| 403 | FORBIDDEN、ORIGIN_REJECTED | 修正权限或来源 |
| 404 | ROOM_NOT_FOUND | 不自动重试 |
| 409 | USERNAME_EXISTS、ROOM_CLOSED | 不自动重试 |
| 415 | UNSUPPORTED_MEDIA_TYPE | 使用 application/json |
| 429 | RATE_LIMITED、CAPACITY_EXCEEDED | error.retryable=true，按 Retry-After 秒数退避 |
| 503 | DEPENDENCY_UNAVAILABLE、RECOVERING | error.retryable=true，按 Retry-After 退避 |
| 501 | CAPABILITY_NOT_ENABLED | 当前阶段尚未交付该能力 |
| 500 | INTERNAL_ERROR | 保留 trace_id，不返回内部堆栈 |

数据库、Redis 故障不得返回 200 加伪造成功值。历史功能在 P6 之前明确返回 CAPABILITY_NOT_ENABLED。握手票据错误对应的握手响应见 WebSocket 协议。

## 运维接口

GET /healthz 返回 200 和进程存活状态，不访问所有中间件。GET /readyz 返回 200 或 503，并提供当前阶段、必要依赖和恢复状态；两者不使用业务响应中的用户身份，也不暴露配置秘密。健康接口从 P1a 建立。

Vite 开发代理统一转发 /api、/ws 和健康接口。生产静态托管与 API 同源；需要额外来源时使用精确 Origin 白名单，不设置携带凭据的通配跨域规则。

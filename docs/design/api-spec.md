# LiveStream HTTP 接口契约

基线 V1.1。负责 HTTP 字段、认证、错误和分页。业务口径见 [领域模型](domain-model.md)，长连接消息见 [WebSocket 协议](websocket-protocol.md)。

## 通用约定

业务接口路径前缀为 /api/v1，JSON 使用 UTF-8。user_id、room_id、记录 id、like_count 和递增版本号均为十进制字符串，避免 JavaScript 整数精度丢失；ID 为无前导零的正整数，计数和版本可为 "0"。online_count、limit 和持续秒数为整数。HTTP 时间为 UTC RFC 3339，固定毫秒精度。

业务接口成功响应为 {"data": ... , "trace_id": "..."}。错误响应为 {"error": {"code": "...", "message": "...", "retryable": false}, "trace_id": "..."}。trace_id 是诊断标识，不是业务幂等键。输入不接受未定义字段。运维探针使用下文定义的独立响应。

除注册、登录、健康和就绪接口外，请求携带 Authorization: Bearer 访问令牌。Content-Type 不匹配返回 415，格式错误返回 400，权限不足不能通过客户端传递用户 ID 绕过。

## 认证和凭据

访问令牌为 JWT，算法固定 HS256，校验签名、iss=livestream-api、aud=livestream-browser、sub、iat 和 exp，拒绝客户端选择其他算法。sub 为用户 ID 字符串；iat 和 exp 必须存在，exp 晚于 iat 且二者间隔不超过 30 分钟，允许最多 30 秒时钟偏差。签名密钥使用至少 32 随机字节的标准 Base64 编码，经 LIVESTREAM_JWT_SECRET 注入，无可运行的默认密钥；解码后的字节用于签名。

前端将令牌保存在内存，刷新页面后重新登录；V1 不实现刷新令牌。退出登录清空内存并关闭该页面连接，不宣称已经撤销其他客户端持有的 JWT。过期长连接按 WebSocket 协议关闭。

密码使用 Argon2id，初始参数 m=19456 KiB、t=2、p=1，每条密码独立 16 字节随机盐和 32 字节输出，保存带参数的编码结果；参数可在性能验证后提高。此起点参考 [OWASP 密码存储指南](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)。登录失败统一返回 INVALID_CREDENTIALS，日志不保存密码、令牌或原始票据。

注册与登录在 P1b 即采用有界的进程内限流，每个来源 IP 每 60 秒合计最多 10 次尝试，最多并行执行 4 个密码计算；达到边界返回 429。P4 起可将窗口计数迁入 Redis，避免将 Redis 提前设为登录前置依赖。

当前窗口按 Unix 时间对齐整分钟，最多保存 10000 个来源 IP，窗口切换时清空上一窗口计数，进程重启也会清空。仅使用 TCP 对端 IP，不信任 X-Forwarded-For/X-Real-IP；本地 Vite 代理后的浏览器因此共享同一来源。429 携带 Retry-After 秒数，前端等待后由用户主动重试。密码计算名额覆盖本次注册/登录操作，达到上限不排队。未知账号也执行一次同参数密码校验，以减少快速失败带来的响应差异。

当前业务请求预算为 3 秒，MySQL 查询传递该 context；已经开始的 Argon2id 运算会在当前调用内完成，名额在操作结束后释放，不启动脱离请求的计算 goroutine。HTTP 请求体上限 8 KiB，读取超时为 5 秒。请求在 Hertz 协议解析阶段被拒绝时可返回纯文本 400/408/413/431；进入业务 Handler 后统一使用本文 JSON 错误格式。业务响应均设置 Cache-Control: no-store，并返回服务端生成的 X-Trace-ID，与响应及访问日志中的 trace_id 相同。

注册、登录只接受单个 JSON 对象和 username、password 字段，不接受查询参数。注册成功不签发令牌、不自动登录；登录成功后前端用 Bearer 调用 users/me 确认身份。退出只清除当前页面会话，没有服务端 logout 接口或 JWT 撤销表。注册请求超时或网络中断时可能已经写入，前端不自动重发，提示先尝试登录确认结果。

## 接口清单

| 方法与路径 | 请求 | 成功 data 与状态 | 说明 |
| --- | --- | --- | --- |
| POST /api/v1/auth/register | username、password | User，201 | 用户名规范化后唯一；不自动登录 |
| POST /api/v1/auth/login | username、password | access_token、token_type=Bearer、expires_in=1800、User 字段 user，200 | 无效凭据统一 401 |
| GET /api/v1/users/me | 无 | User，200 | P1b 认证联调；从 Bearer 中确定当前用户，拒绝查询参数和请求体 |
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

P2 已实现创建、列表、详情、关闭四个房间接口，均要求 Bearer 认证；输入未知字段、重复查询参数和非法 ID 被拒绝。房间票据、统计和历史接口仍为后续阶段契约。

数据库、Redis 故障不得返回 200 加伪造成功值。历史功能在 P6 之前明确返回 CAPABILITY_NOT_ENABLED。握手票据错误对应的握手响应见 WebSocket 协议。

## 运维接口

GET /healthz 返回 200、JSON 响应 {"status":"ok"} 和 Cache-Control: no-store，仅检查进程能否响应，不访问中间件。不支持的方法返回 405。

GET /readyz 返回 JSON 和 Cache-Control: no-store。当前 P2 数据库接入后的响应字段如下：

| 字段 | 当前含义 |
| --- | --- |
| status | starting、ready、not_ready 或 stopping；仅 ready 返回 200，其余返回 503 |
| stage | 固定为 P2，表示房间管理阶段，不代表实时连接或点赞已完成 |
| dependencies | 仅包含 mysql：未检查为 not_checked，查询和版本检查通过为 ready，失败为 unavailable；不包含未接入的 Redis、Kafka |
| recovery | not_applicable；本阶段没有投影恢复链路 |

API 在监听前检查 MySQL 连接、迁移版本和所需列，失败直接退出，因此初始化失败通常表现为连接不上 API。运行期间，每次就绪请求在 1 秒预算内重新执行只读检查；失败返回 not_ready/503，数据库恢复并通过检查后可重新返回 ready/200。检查不读取账号记录，也不证明所有索引、约束或业务功能均正确。

开始停止时生命周期改为 stopping，之后不能重新变为 ready；探针在数据库检查后再次核对停止状态。不支持的方法返回 405。后续阶段接入其他依赖和恢复检查时同步扩展契约与验收。

两种探针均不要求用户身份，也不暴露配置秘密。未就绪不改变 /healthz 的存活含义；开始关闭网络监听后，客户端也可能直接收到连接失败，不能保证一定观察到 503。实际完成情况见项目状态。

Vite 开发代理统一转发 /api、/ws 和健康接口。生产静态托管与 API 同源；需要额外来源时使用精确 Origin 白名单，不设置携带凭据的通配跨域规则。

# LiveStream HTTP 接口契约

基线 V1.1。负责 HTTP 字段、认证、错误和分页。业务口径见 [领域模型](domain-model.md)，长连接消息见 [WebSocket 协议](websocket-protocol.md)。

看实现时，先从 [Hertz 路由注册](../../backend/internal/httpserver/server.go) 找入口，再分别阅读 [认证](../../backend/internal/httpserver/auth.go)、[房间](../../backend/internal/httpserver/room.go)、[实时票据与榜单](../../backend/internal/httpserver/realtime.go) 和[弹幕查询](../../backend/internal/httpserver/danmaku.go)。接口表中标记“目标”的行尚未注册，示例不是已完成的证据。

## 通用约定

业务接口路径前缀为 /api/v1，JSON 使用 UTF-8。user_id、room_id、session_id、记录 id、like_count 和递增版本号均为十进制字符串，避免 JavaScript 整数精度丢失；ID 为无前导零的正整数，计数和版本可为 "0"。online_count、limit 和持续秒数为整数。HTTP 时间为 UTC RFC 3339，固定毫秒精度。

业务接口成功响应为 {"data": ... , "trace_id": "..."}。错误响应为 {"error": {"code": "...", "message": "...", "retryable": false}, "trace_id": "..."}。trace_id 是诊断标识，不是业务幂等键。输入不接受未定义字段。运维探针使用下文定义的独立响应。

除注册、登录、健康、就绪和运行指标接口外，请求携带 Authorization: Bearer 访问令牌。Content-Type 不匹配返回 415，格式错误返回 400，权限不足不能通过客户端传递用户 ID 绕过。

启用任一实时业务后，API 的 `GET /metrics` 提供 Prometheus 抓取接口。时间序列按实例和固定结果聚合，房间、场次、用户与连接 ID 不作为标签。生产反向代理仅向监控网开放此路径。指标口径见[验证设计](../verification/plan.md#实时点赞运行指标)。

## 认证和凭据

访问令牌为 JWT，算法固定 HS256，校验签名、iss=livestream-api、aud=livestream-browser、sub、iat 和 exp，拒绝客户端选择其他算法。sub 为用户 ID 字符串；iat 和 exp 必须存在，exp 晚于 iat 且二者间隔不超过 30 分钟，允许最多 30 秒时钟偏差。签名密钥使用至少 32 随机字节的标准 Base64 编码，经 LIVESTREAM_JWT_SECRET 注入，无可运行的默认密钥；解码后的字节用于签名。

前端将访问令牌保存在同源 localStorage，键为 `livestream.access-token`，不保存密码。刷新或重新打开页面时先检查 JWT 的原始 exp，再用 users/me 验证身份；恢复登录不签发新令牌、不重置有效期。前端读取 exp 仅用于界面定时退出，身份、签名和有效期仍由服务端校验。V1 不实现刷新令牌。

退出登录或接口返回 401 时清空内存和本地凭证；同源标签页通过 storage 事件同步退出及账号切换，其他浏览器独立。恢复时网络/数据库故障保留未到期凭证并显示重试入口；损坏或过期凭证清理后重新登录。浏览器禁止存储时允许本页登录并提示无法保持登录。退出不宣称撤销其他客户端持有的 JWT；过期长连接按 WebSocket 协议关闭。

本地开发版沿用 Bearer 接口，以短期凭证持久化满足刷新恢复；没有引入 Cookie 会话或自动续期协议。localStorage 可被同源脚本读取，不能防御 XSS，页面必须保持文本转义，不引入不可信脚本；不得把密码放入存储、URL 或日志。

密码使用 Argon2id，初始参数 m=19456 KiB、t=2、p=1，每条密码独立 16 字节随机盐和 32 字节输出，保存带参数的编码结果；参数可在性能验证后提高。此起点参考 [OWASP 密码存储指南](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)。登录失败统一返回 INVALID_CREDENTIALS，日志不保存密码、令牌或原始票据。

注册与登录在 P1b 即采用有界的进程内限流，每个来源 IP 每 60 秒合计最多 10 次尝试，最多并行执行 4 个密码计算；达到边界返回 429。P4 起可将窗口计数迁入 Redis，避免将 Redis 提前设为登录前置依赖。

当前窗口按 Unix 时间对齐整分钟，最多保存 10000 个来源 IP，窗口切换时清空上一窗口计数，进程重启也会清空。仅使用 TCP 对端 IP，不信任 X-Forwarded-For/X-Real-IP；本地 Vite 代理后的浏览器因此共享同一来源。429 携带 Retry-After 秒数，前端等待后由用户主动重试。密码计算名额覆盖本次注册/登录操作，达到上限不排队。未知账号也执行一次同参数密码校验，以减少快速失败带来的响应差异。

当前业务请求预算为 3 秒，MySQL 查询传递该 context；已经开始的 Argon2id 运算会在当前调用内完成，名额在操作结束后释放，不启动脱离请求的计算 goroutine。HTTP 请求体上限 8 KiB，读取超时为 5 秒。请求在 Hertz 协议解析阶段被拒绝时可返回纯文本 400/408/413/431；进入业务 Handler 后统一使用本文 JSON 错误格式。业务响应均设置 Cache-Control: no-store，并返回服务端生成的 X-Trace-ID，与响应及访问日志中的 trace_id 相同。

注册、登录只接受单个 JSON 对象和 username、password 字段，不接受查询参数。注册成功不签发令牌、不自动登录；登录成功后前端用 Bearer 调用 users/me 确认身份。没有服务端 logout 接口或 JWT 撤销表。注册请求超时或网络中断时可能已经写入，前端不自动重发，提示先尝试登录确认结果。

## 接口清单

| 方法与路径 | 请求 | 成功 data 与状态 | 说明 |
| --- | --- | --- | --- |
| POST /api/v1/auth/register | username、password | User，201 | 用户名规范化后唯一；不自动登录 |
| POST /api/v1/auth/login | username、password | access_token、token_type=Bearer、expires_in=1800、User 字段 user，200 | 无效凭据统一 401 |
| GET /api/v1/users/me | 无 | User，200 | P1b 认证联调；从 Bearer 中确定当前用户，拒绝查询参数和请求体 |
| POST /api/v1/rooms | title | Room，201 | owner_id 来自令牌 |
| GET /api/v1/rooms | status、owner、limit、cursor | items: Room[]、next_cursor，200 | status 默认 OPEN，可为 CLOSED；owner 可省略或为 me；按 id 倒序 |
| GET /api/v1/rooms/{id} | 无 | Room，200 | 包含关闭房间 |
| POST /api/v1/rooms/{id}/close | 空 JSON 对象 | Room，200 | 仅房主结束本轮直播；连续重复关闭保留本轮 closed_at |
| POST /api/v1/rooms/{id}/reopen | 空 JSON 对象 | Room，200 | 仅房主；原房间恢复 OPEN，closed_at=null；已 OPEN 时返回当前房间 |
| POST /api/v1/rooms/{id}/ws-ticket | 空 JSON 对象 | ticket、expires_at、ws_path、session_id，201 | 仅开放房间；规则见下文 |
| GET /api/v1/rooms/{id}/likes-state | 无 | state、session_id、room_status，200 | 读取房间与当前场次，供点赞前端识别恢复状态 |
| GET /api/v1/rooms/{id}/sessions/{session_id}/like-leaderboard | Bearer，无查询参数 | 本场榜单与本人累计数，200 | 登录用户可读，首次进入与补取共用 |
| GET /api/v1/rooms/{id}/danmaku | 可选 session_id、limit、cursor | items: Danmaku[]、next_cursor、consistency=eventual，200 | 已实现；已落库数据按 id 倒序，开放或关闭房间均可查询 |
| GET /api/v1/rooms/{id}/danmaku/recent | 必需 session_id，可选 cursor | items、next_cursor，200 | 当前场次最近缓存；每次最多 20 条，详见下文 |
| GET /api/v1/rooms/{id}/stats | 无 | RoomStats，200 | **目标，尚未实现或注册**；下方示例为目标设计 |

User 字段：id、username、created_at。Room 字段：id、owner_id、title、status、created_at、closed_at；closed_at 在 OPEN 时为 null。Danmaku 字段：id、event_id、request_id、room_id、session_id、user_id、username、content、created_at；username 是发送时快照。V2 的历史、实时广播和最近缓存的 created_at 是服务端接收时间；缺少该字段的 V1 旧事件仍显示客户端事件时间。

创建房间未提供 HTTP 幂等键，前端禁止重复点击和自动重发。结果未知时先刷新本人房间列表进行确认，不能将网络超时直接解释为创建失败。弹幕与点赞的幂等协议仅适用于 WebSocket 业务请求。

重新开播保持 id、owner_id、title、created_at，下一次关闭重新设置该轮 closed_at。开播/关闭在事务中对同一房间加行锁，响应表示本次事务完成时的状态，随后仍可能被另一请求改变。连续同方向操作幂等；中间发生反向操作后再次请求代表新操作，不提供跨直播轮次的请求去重。前端不自动重发，结果未知时先刷新详情再操作。

## 票据接口

Bearer 认证通过、Origin 合法且房间开放时，签发 32 随机字节的 base64url 票据。有效期为 30 秒且不超过 JWT 剩余有效期；exp 已到时不利用时钟宽限继续签发票据。只在当前 API 内存保存票据摘要、用户及用户名快照、房间、签发 Origin 和令牌到期时间。

响应不缓存，设置 Cache-Control: no-store。ws_path 形如 /ws/rooms/1001?ticket=短时票据，前端根据当前环境补充 ws 或 wss 地址。原始票据只用于一次握手，握手失败后重新申请。不能将 access_token 放入此 URL。

票据数量上限见 [架构容量边界](architecture.md)；超限拒绝申请，不无限缓存。API 重启会使未使用票据失效，客户端重新申请即可。浏览器 WebSocket 构造函数没有自定义请求头参数，因此采用 HTTP 换票方式，见 [MDN API](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/WebSocket)。

## 本场点赞榜

房间必须开放，路径里的场次必须是当前这一场。接口通过登录令牌确认本人身份，返回带有 `Cache-Control: no-store` 的完整快照。下例中，当前用户还未点赞。

```json
{"data":{"room_id":"8","session_id":"11","projection_epoch":"a1b29c9b-91dc-4162-9285-84803df6649c","revision":"5","likes_revision":"5","top":[{"rank":1,"user_id":"2","username":"alice","count":"5"}],"me":{"count":"0"}}}
```

`top` 按名次返回 0–100 行。ID、计数和版本都用十进制字符串，名次为从 1 开始的 JSON 整数。本人在榜时 `me` 包含 `rank`，榜外只返回 `count`。同一场次与投影代次内，`revision` 表示最近一次公共榜单变化，`likes_revision` 表示读取个人数时的场次版本。榜外用户增加点击数时，两个版本可以不同。

未登录返回 401。无效场次编号或附加查询参数返回 400，房间不存在返回 404，场次已结束或与当前房间不匹配返回 409。投影或依赖不可用返回 503。成功读取且无人点赞时才返回空榜。

## 统计响应（目标，尚未实现）

```json
{
  "data": {
    "room_id": "1001",
    "session_id": "3001",
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

目标 RoomStats 将包含以上字段，session_id 与 like_count 指向同一场次，旧场统计不并入新场。该接口和在线数目前均未实现，因此示例中的字段不是当前响应。点赞能力已由已实现的 `likes-state`、榜单接口和 WebSocket 状态提供；投影不可用时不能把缺失计数当作零。

## 分页

房间列表和 MySQL 弹幕历史默认 limit=20，允许 1 至 100；超过范围返回 INVALID_ARGUMENT。cursor 为服务端生成的不透明 base64url 字符串，包含上页末尾 id 及绑定的过滤条件；解码后逐项校验，不直接拼接 SQL。筛选条件改变时旧游标返回 INVALID_CURSOR。Redis 最近缓存使用独立游标和固定最多 20 条，见[最近弹幕缓存](#最近弹幕缓存)。

弹幕可按 session_id 筛选；该场次必须属于路径房间，否则返回 404。未指定时查询该房间所有场次。重复、未知或显式为空的查询参数返回 400。游标绑定 room_id 与 session_id；更改筛选需重新开始分页。弹幕以数据库自增 id 作为稳定分页位置，表示数据库插入分配顺序，不等同于严格提交顺序或 Kafka 发送顺序，不能据此推断跨客户端发送的绝对先后。next_cursor 无下一页时为 null，空页为 items=[]。实时流与历史页通过 event_id 合并。

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

当前已实现创建、列表、详情、关闭、重新开播、房间票据、点赞状态/榜单和弹幕历史，均要求 Bearer 认证；stats 统计接口仍未实现。

数据库、Redis 故障不得返回 200 加伪造成功值。弹幕历史在启用弹幕子系统时注册，词库错误不关闭历史查询。握手票据错误对应的握手响应见 WebSocket 协议。

## 运维接口

GET /healthz 返回 200、JSON 响应 {"status":"ok"} 和 Cache-Control: no-store，仅检查进程能否响应，不访问中间件。不支持的方法返回 405。

GET /readyz 返回 JSON 和 Cache-Control: no-store。status 按 HTTP 生命周期、MySQL 连接与结构检查判定 ready/200 或 starting、not_ready、stopping/503。dependencies 只包含 mysql；stage 当前保留 P4-likes 兼容值（启用共享实时路由时）；recovery 独立报告点赞 CHECKING、READY、RECOVERING、BLOCKED，无实时路由时为 not_applicable。danmaku 独立报告 READY、UNAVAILABLE 或 DISABLED。业务不可用不改变普通 API 的状态码。此接口不检查独立 Worker；Worker 状态通过其日志、独立 /metrics、消费组和数据库检查点核对。

GET /api/v1/rooms/{id}/likes-state 要求 Bearer，沿用普通响应外壳，返回 data={state,session_id,room_status}。state 为上述四种点赞状态；开放房间的 session_id 为十进制字符串，关闭时为 null。此接口只依赖 MySQL，可用于恢复期间首次取得场次并暂存点击；未启用点赞时 state 为 BLOCKED。该接口不向页面泄露原始错误、地址和凭据。

ws-ticket 与 WebSocket 握手不依赖点赞 READY；恢复期间可建立共享连接，首屏分别报告业务能力与未知计数。启用点赞且恢复期间，房间创建、关闭、重开仍返回 503/LIKES_UNAVAILABLE，以保持点赞重建时场次集合稳定。登录、注册和房间读取按自身依赖正常服务。

API 在监听前检查 MySQL 连接、迁移版本和所需列，失败直接退出，因此初始化失败通常表现为连接不上 API。运行期间，每次就绪请求在 1 秒预算内重新执行只读检查；失败返回 not_ready/503，数据库恢复并通过检查后可重新返回 ready/200。检查不读取账号记录，也不证明所有索引、约束或业务功能均正确。

开始停止时生命周期改为 stopping，之后不能重新变为 ready；探针在数据库检查后再次核对停止状态。不支持的方法返回 405。后续阶段接入其他依赖和恢复检查时同步扩展契约与验收。

两种探针均不要求用户身份，也不暴露配置秘密。未就绪不改变 /healthz 的存活含义；开始关闭网络监听后，客户端也可能直接收到连接失败，不能保证一定观察到 503。实际完成情况见项目状态。

Vite 开发代理统一转发 /api、/ws 和健康接口。生产静态托管与 API 同源；需要额外来源时使用精确 Origin 白名单，不设置携带凭据的通配跨域规则。

## 最近弹幕缓存

`GET /api/v1/rooms/{id}/danmaku/recent` 使用 Bearer 认证，要求 `session_id`，可携带不透明 `cursor`。正常请求只查询 Redis 的场次缓存，每页最多返回 20 条，按缓存时间倒序，返回 `items` 和 `next_cursor`。满页可能带有游标，即使下一页为空；游标对应记录过期后分页结束。缓存未就绪时返回可重试的 `CACHE_WARMING`，由后台恢复任务按屏障读取 MySQL；普通请求不回退 MySQL。直播连接建立后前端自动读取，滚动到顶部或点击加载更多时读取下一页。缓存事件来自 Kafka 消费或 MySQL 恢复，可用匹配完整身份与内容的记录确认请求已接受，但不能证明 MySQL 已落库。

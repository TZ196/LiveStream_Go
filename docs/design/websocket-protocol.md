# LiveStream WebSocket 协议

基线 V1.1，协议 version=1。共享连接支持单账号归属、在线人数、点赞与弹幕独立能力及历史补查。请求幂等算法见 [可靠性设计](reliability.md)，票据签发见 [HTTP 接口](api-spec.md#票据接口)。

阅读路径：先看 [HTTP 票据路由](../../backend/internal/httpserver/realtime.go)和[一次性票据](../../backend/internal/realtime/ticket.go)，再看 [manager.go](../../backend/internal/realtime/manager.go) 的握手与读循环、[delivery.go](../../backend/internal/realtime/delivery.go) 的单连接写循环。消息分别进入 [点赞接入](../../backend/internal/realtime/likes.go)或[弹幕接入](../../backend/internal/realtime/danmaku.go)；浏览器连接、点赞和弹幕状态集中在 [realtime.ts](../../frontend/src/realtime.ts)、[likes.ts](../../frontend/src/likes.ts) 与 [danmaku.ts](../../frontend/src/danmaku.ts)。

## 握手及连接生命周期

1. 前端带 Bearer 请求房间 ws-ticket，指定主动进入或恢复原观看。签发时核对归属并绑定观看意图。
2. 连接 /ws/rooms/{room_id}?ticket=...；服务端先检查路径、Origin 白名单、票据摘要、绑定的用户及房间、票据和 JWT 到期时间。
3. 原子消费票据，为账号预留握手及连接名额。检查开放场次后升级，已消耗的票据用毕即失效。
4. 在准入锁下确认场次和 API 状态。Redis 确认新归属后，RoomHub 同步停用旧连接、切换本地登记，唯一写循环先发 `ROOM_STATE`。前端收到有效首屏才激活收发。已通过准入的旧请求继续按原场次处理。
5. 识别退出后按连接身份摘除成员，读写循环退出后归还实际连接名额。旧连接重复清理不改变新连接和人数。

票据签发按 HTTP 错误外壳返回。无效、过期、已用票据或 Origin 不符返回 `401`。房间关闭或场次改变返回 `409`。同账号并发握手返回 `429`，单实例实际连接满额返回 `503`。场次和归属在升级后再次检查，失败时通过关闭帧退出。升级及登记前失败时，当前观看继续。

### Redis 归属与租约

`ws:owner:{user_id}` 的 JSON 包含 `user_id`、`instance_id`、`connection_id`、`room_id`、`session_id`、`watch_id`、`generation`、Redis `run_id` 和 `active`。实例与物理连接各用独立 UUID。观看编号在同一次观看的重连中保留，主动进入会产生新编号。`generation` 在键存续期间递增。判断归属时还需核对唯一的连接编号，避免键过期后代次重用造成误删。

[登记脚本](../../backend/internal/realtime/lua/claim_owner.lua) 比较签发时的观看归属，拒绝被随后成功接管的旧票据。恢复模式另核对原连接、房间和场次。写入超时只按本次连接编号查询结果。登记已成功而首屏未送达时，客户端可凭原观看编号恢复。`release_owner.lua` 比较归属后将 `active` 设为 `false` 并短留 30 秒，期间人数已扣除。`renew_owner.lua` 只给匹配的有效记录续期。

租约 30 秒，每 10 秒按本机活跃连接分批续期。短时读写失败只使用前一次确认的剩余时间，超过本地截止就停用。确认 Redis `run_id` 变化或键失效后停止旧连接。Redis 不可用时拒绝新的归属登记，JWT 到期照常关闭。WebSocket Ping/Pong 用于检测网络连接，和 Redis 归属续期独立。

同一实例直接在 RoomHub 停用旧连接。跨实例时通过 `ws:replace:<instance_id>` 尽力通知目标节点，匹配完整旧记录后关闭，漏收由续期校验发现。[Redis Pub/Sub 投递语义](https://redis.io/docs/latest/develop/pubsub/#delivery-semantics)。本轮验收为单 API 互斥。跨节点通知只提供归属控制和有界收敛，跨节点实时广播、全局人数与严格接管时序留待多实例设计。

## 帧约定

使用 UTF-8 文本 JSON 帧，拒绝非法 UTF-8，不接收二进制业务帧。version 必须为 1；客户端消息只接受定义的字段。单消息最多 8 KiB，超出使用关闭码 1009。弹幕不接受 NUL 和除制表符、换行外的控制字符；文本只作为文本渲染。

业务 request_id 为规范小写 UUIDv7，由客户端在消息产生时生成并保留；事件时间从该 ID 提取，客户端时钟需满足校验窗口。每条弹幕和每个点赞批次各有一个 request_id。重试沿用原 request_id、session_id、消息类型与 data。DANMAKU.data 只有 content。LIKE.data 只有 count，取值为正的 JSON 整数，须可表示为有符号 64 位整数。兼容旧客户端的空对象 `{}`，按 1 次点击处理。服务端接收完整批次，未设置点击频率或单批点击数的业务上限。room_id 来自连接；客户端传入原始 session_id 以保证重试不意外改绑，服务端验证其来源与授权，不能仅因客户端提供就信任。场次结束后的旧请求不能改绑新场；弹幕利用冻结状态或持久化历史核对结果，缺证据时保持未确认。点赞的长期旧请求查询仍待后续设计。

```json
{
  "version": 1,
  "type": "DANMAKU",
  "session_id": "3001",
  "request_id": "01a1edab-7800-7abc-8000-123456789abc",
  "data": {"content": "大家好"}
}
```

```json
{
  "version": 1,
  "type": "LIKE",
  "session_id": "3001",
  "request_id": "01a1edab-7801-7abc-8000-123456789abc",
  "data": {"count": 5}
}
```

上述 UUID 仅为 2026 年 11 月 18 日 08:00 UTC 的示例，执行测试时生成符合当前请求窗口的新值。

## 接受和错误

```json
{
  "version": 1,
  "type": "ACK",
  "request_id": "01a1edab-7800-7abc-8000-123456789abc",
  "event_id": "a61f1c41-7c5f-514d-acb9-c9a4f6b3241a",
  "data": {
    "status": "accepted",
    "accepted_via": "kafka",
    "accepted_at": "2026-11-18T08:00:00.010Z"
  }
}
```

当前弹幕与点赞 ACK 均为 accepted_via=kafka；不运行 memory 弹幕备用路径。重复请求返回相同 event_id；没有请求登记时无法保证首次接受时间一致，accepted_at 可以为 null。

Kafka 结果不确定时不向前端发送包含 Kafka 确认丢失细节或 RESULT_UNKNOWN 的消息，也不伪造成功或明确失败；服务端在预算内使用相同事件有界重试，前端未收到 ACK 时保留原请求并按有限重试规则处理，不把后续点击并入旧请求。重试耗尽仍无结果时停止自动重发；后台保留诊断。长期未确认的安全查询或处理机制需在上线前确定，不得假定消息未发生。

弹幕 ERROR 明确区分 outcome=rejected 与 pending；仅有确切拒绝依据时使用 rejected。顶层 request_id、event_id 无法确定时为 null；data 包含 code、message、outcome、retryable、retry_after_ms。不能因网络中断或记录不可读就断言操作未发生。

| code | outcome | 客户端行为 |
| --- | --- | --- |
| INVALID_ARGUMENT、UNSUPPORTED_MESSAGE | rejected | 不自动重试；修改内容属于新的用户动作 |
| CLOCK_SKEW | rejected | 校正客户端时间，禁止自动修改未知操作的 ID |
| REQUEST_EXPIRED | rejected | 仅能证明从未接受时返回；无法确定历史结果时后台保留未确认并停止自动重发，不对前端发送未知结果 |
| ROOM_CLOSED、ROOM_NOT_FOUND、AUTH_EXPIRED | rejected | 离开房间或重新认证 |
| RATE_LIMITED、SERVER_BUSY、DEPENDENCY_UNAVAILABLE、RECOVERING | rejected | 按 retry_after_ms 用原请求重试；此响应仅在尚未开始发布时使用 |
| REQUEST_IN_PROGRESS | pending | 保留原请求，按 retry_after_ms 有限重试；记录缺失时不重造事件 |
| CONTENT_BLOCKED | rejected | 只给简短审核未通过提示，移除本地拒绝记录；输入框空闲且仍为最后一次提交时恢复草稿，用户修改后主动发送才生成新请求 |
| REQUEST_CONFLICT | rejected | 原 ID 已用于其他内容/场次，不自动更换 ID |
| DANMAKU_UNAVAILABLE | rejected 或 pending | 依 outcome 保留草稿或原请求，稍后核对，不影响点赞 |
| CAPABILITY_NOT_ENABLED | rejected | 当前阶段隐藏未交付操作 |

outcome 描述当前业务动作的已知结果。不能因本次未执行就宣称原操作从未发生，也不向用户暴露 Kafka 内部 UNKNOWN。弹幕 Redis 原子登记可在发布前拒绝相同 ID 的不同内容；点赞仍保持消费端冲突保护。当前点赞发布失败保持无 ACK；没有顺带补齐点赞 ERROR 帧。

## 服务端消息

所有消息含 version、type、data。下表给出此外的字段和 data 的完整业务字段；HTTP 公共字段类型约定同样适用。

| type | 顶层附加字段 | data |
| --- | --- | --- |
| ROOM_STATE | 无 | room_id、session_id、status、online_count、server_instance_id、presence_revision、like_count、likes_available、likes_state、projection_epoch、projection_generation、likes_revision、danmaku_available、delivery_modes、history_available、delivery_probe |
| DANMAKU_BROADCAST | event_id、request_id | room_id、session_id、user_id、username、content、created_at |
| LIKE_UPDATE | 无 | room_id、session_id、like_count、projection_epoch、likes_revision |
| LIKE_BOARD_DELTA | 无 | room_id、session_id、projection_epoch、base_revision、revision、size、changes |
| LIKE_BOARD_REFRESH | 无 | room_id、session_id、projection_epoch、revision |
| MY_LIKES_UPDATE | 无 | session_id、projection_epoch、likes_revision、count，只送本人在本场的连接 |
| ROOM_ONLINE_UPDATE | 无 | room_id、session_id、online_count、server_instance_id、presence_revision |
| ROOM_CLOSED（通知后续补齐） | 无 | room_id、closed_at、reason=owner_closed；当前关房关闭 socket，前端重新查询确认 |
| PONG | 无 | nonce、server_time_ms |
| ACK、ERROR | 按前文 | 按前文 |

ROOM_STATE 每个业务单独报告能力。delivery_modes.LIKE/DANMAKU 在启用时为 "kafka"，未启用为 null；弹幕暂不可用时仍保留已启用的模式。history_available 表示弹幕历史路由存在，词库错误不关闭历史。status 为 OPEN，在线数与 HTTP RoomStats 共用 RoomHub 的口径。

点赞状态变化或弹幕能力变化后，共享连接收到新的 ROOM_STATE。projection_generation 为本 API 进程内递增的十进制字符串；前端在同一物理连接拒绝较小 generation，在新连接重新建立比较基线。epoch 改变后清空旧点赞/榜单基线，LIKE_UPDATE 和榜单仅采用当前 epoch。点赞恢复不关闭共享连接；LIKES_UNAVAILABLE 只暂停点赞采集后的发送，不暂停弹幕。

弹幕广播按 event_id 去重。点赞 ACK 只确认发送者对应的批次。LIKE_UPDATE 是全场绝对总数的快照，可以覆盖多个用户的多个批次。快照和 ACK 可以先后独立到达，只有 ACK 能移除待确认批次。

likes_revision 为同一 projection_epoch 及 session_id 内的单调递增字符串整数，每个首次处理的批次按 count 增加，重复事件保持原值。API 每 200 毫秒向有观众且有变化的场次推送一次最新快照，空闲周期没有消息。客户端在同一场次、同一投影代次只采用较新或相同的版本。

人数改变时每秒最多合并推送一次。`presence_revision` 从进程级递增序列分配，空场回收后重新建立也使用更高版本。同场换窗口且人数不变时沿用当前版本。`server_instance_id` 随 API 进程重新生成。浏览器在本次连接内按实例、场次和版本过滤迟到人数帧，新连接从首屏建立基线。

每次加入或重连，前端收到带场次和投影代次的 ROOM_STATE 后，从 HTTP 获取本场榜单和本人累计数。API 每秒合并一次公共榜单变化。LIKE_BOARD_DELTA 中的 changes 为发生变化的完整名次行，字段与 HTTP top 一致。客户端从 base_revision 对应的榜单替换这些名次，将长度设为 size，再采用 revision。基准版本缺失、补丁无效或收到 LIKE_BOARD_REFRESH 时，重新获取一次 HTTP 快照。差量帧编码后超过 8192 字节时服务端只发刷新通知。

MY_LIKES_UPDATE 带最终累计数，包括榜外用户的点击，只发给该用户在本场的有效连接。个人数按 likes_revision 比较，公共榜单按 revision 比较。两者独立过滤迟到数据，换场或换代后重新取得快照。

## 心跳及退出

服务端每 20 秒发送 WebSocket 控制 Ping，浏览器协议栈响应控制 Pong，60 秒读期限用于检测断线。每条连接的发送队列容量为 128。遇满立即封闭本连接的入队入口，由已有连接协程关闭 socket。正在执行的写入有 5 秒期限。

### 点赞总数送达采样

API 从第一个连接起每 64 条新连接选一条，在首屏 `ROOM_STATE.data.delivery_probe` 标明。只有识别此能力的客户端才启用校时。控制消息使用 version=1、当前 session_id，无业务 request_id。

1. 浏览器立即发送应用层 `PING`，之后每 30 秒再发一次。data.nonce 为 1 至 64 字节的字符串。服务端 `PONG.data` 返回相同 nonce 和 `server_time_ms`（处理 PING 时的 Unix 毫秒）。浏览器以发出和收到之间的单调时间中点对齐服务器时间。60 秒后校时过期，连接切换时清除。
2. 对已启用采样的连接，写协程最多每 30 秒在一条 `LIKE_UPDATE` 顶层加入 `server_sent_at_ms`，取自即将编码并写出该帧时的服务端时钟。浏览器在 message 回调入口记下单调时间，用独立的校时结果换算成服务端时间。
3. 浏览器尽力回传 `DELIVERY_RECEIPT.data`，原样带回 `server_sent_at_ms`。有效校时还携带 `received_at_ms` 和 `uncertainty_ms`。误差估计包括校时半往返、1 ms 量化余量及按 200 ppm 估计的漂移。主线程等待也计入接收延迟。回执不等待 Vue 更新或屏幕绘制。
4. 每条连接只保留一个待回执样本。服务端只接纳本连接匹配且 15 秒内的首次回执。缺少校时或误差估计超过 50 ms 时只统计服务端单调时钟往返。超出往返可解释区间的值单独计数。误差内的负单程估计按零记录。查看单程分位数时须同时核对误差分布、排除数及未回执数。旧客户端仍可接收常规帧。控制消息不进入 Kafka，不影响业务 ACK 或重试。

服务端尽力发送以下关闭原因。浏览器收不到关闭帧时按普通网络断开处理，恢复票据仍受 Redis 归属校验。

| 关闭码 | 含义 | 是否自动重连 |
| --- | --- | --- |
| 1000 | 用户离开 | 否 |
| 1008 | 协议或来源策略拒绝 | 否 |
| 1009 | 消息过大 | 否 |
| 1012 | 服务停止 | 退避并尝试恢复原观看 |
| 1013 | 资源繁忙、慢连接或本地无法续租 | 退避并尝试恢复原观看 |
| 4001 | 认证到期 | 先重新登录 |
| 4004 | 房主关闭房间 | 否 |
| 4009 | 当前观看已被替换 | 否，保留登录，用户可主动重新进入 |
| 4010 | 观看或票据意图已失效 | 否，用户可主动重新进入 |

错误或关闭通知尽力发送，不能保证断网客户端收到。按关闭码和重新查询的房间状态决定后续行为。

## 前端责任

HTTP 与 WebSocket 分别集中封装，页面只使用明确的业务状态。断线重连基准为 1、2、4、8、16、30 秒，乘以 0.8–1.2 的随机系数，实际等待最多 30 秒。收到首个有效 ROOM_STATE 后连接稳定三十秒才重置退避，后续能力快照不重置它，避免慢连接刚握手又断开时始终快速重试。同一页面只建立一条房间连接，两项业务分别订阅和发送。主动离开、房间关闭、认证失效或组件卸载时取消重试和稳定计时器、关闭连接；离开页面时由业务订阅方清空监听器。重连不恢复旧连接的发送队列，最近五分钟的 V2 弹幕通过缓存补查，旧 V1 或更早消息不保证补回；未确认发送沿用原请求 ID。

同一标签页内按用户、房间和场次聚合点赞。HTTP likes-state 在 Redis 恢复期间也返回当前场次；已确认开放场次后，断线、CHECKING、RECOVERING、BLOCKED 期间继续收集点击，内部保留累计数与待确认记录，页面只给简短连接或恢复提示。服务端权威总数显示在主播头像旁，页面不展示本地待发送、待确认数量。

在线第一下点击开启固定 500 毫秒窗口，到时发送批次。未发送点击以十进制累计数保存，首次发送前才生成 UUIDv7 并冻结数量，保存后调用 send。已尝试发送的批次不再并入新点击，重试沿用 ID、场次、数量。每次连接一轮最多 5 次、ACK 超时 3 秒；建立新物理连接并取得同场次有效 ROOM_STATE 后开启新一轮；同连接能力刷新不会重置重试次数，超过原请求 24 小时则保留记录和未确认提示，不更换 ID 补发。收到 accepted ACK 后清理对应批次，ACK 不代表数据库落盘。

最多保留 100 个冻结待确认批次，窗口满时继续累积未发送计数；计数使用 BigInt 与十进制字符串保存，发出时按 JavaScript 安全整数范围拆分。每 100 毫秒最多发送 10 个批次，并检查 WebSocket bufferedAmount，不将这些保护阈值当作实测容量。

sessionStorage 按用户、房间、场次保存累计数与冻结批次，同标签页刷新可恢复，兼容原数组记录。新场次不加载旧场次缓存。关闭房间、账号改变或原请求过期时不把旧记录转发为新操作。浏览器无法存储时提示本页记录无法持久保存。

共享连接建立前以 likes-state 取得 MySQL 场次，忽略点赞状态门槛；连接保持期间由 ROOM_STATE 能力更新驱动恢复，不再为点赞单独反复关闭连接。网络或连接失败按上述退避；等待恢复不消耗发送次数。只有状态 READY、WebSocket 已开并收到同场次有效 ROOM_STATE 才开始补发，打开 socket 本身不足以允许发送。离开页面取消探测、发送和连接超时计时器，释放连接及监听器。

弹幕前端内部维护最多 20 条待确认记录和 5000 个 event_id 去重，聊天列表最多 200 条。待确认队列按顺序发送，前一条未确认时不自动越过；3 秒 ACK 超时、最多 5 次自动尝试，耗尽后保留原 ID，通过一条简短状态和“重试确认”入口核对；页面不展示队列明细或数量。明确且不可重试的拒绝移出本地队列，CONTENT_BLOCKED 只显示 3.5 秒的通用审核提示；仅当输入框为空且对应最后一次提交时恢复原文草稿，不能覆盖后来输入或提交的内容。修改后用户主动发送生成新 ID，拒绝内容不会自动重发或进入聊天。旧缓存中的 rejected 记录清理，pending/unconfirmed 继续保留原身份。sessionStorage 按用户、房间、场次隔离；旧场缓存不送入新场。输入字符数按 Unicode 码点计算，Enter 发送，Shift+Enter 换行，输入法组合期间不触发发送。

浏览器进入或重连取得 ROOM_STATE 后读取 Redis 最近五分钟缓存，刷新重新建立分页游标，向上滚动或点击按钮继续读取，每页最多 20 条。缓存事件来自 Kafka 消费或 MySQL 恢复；与原请求的用户、房间、场次、请求编号和内容均匹配时，可确认已接受并解除未确认，但不能证明 MySQL 已落库。实时与缓存按 event_id 合并，纯文本展示，不使用 v-html；缓存不可用或消费延迟时不承诺即时消息全部必达。未取得场次编号时前端不发起历史查询。

Redis 请求身份变化后的弹幕保护期、冻结版本和未知结果规则见可靠性设计；前端保留草稿和原请求，不自动改 ID 规避保护。

直播详情采用左侧 16:9 静态演示画面、右侧上方可收起的点赞榜、右侧下方弹幕聊天和固定输入框；窄屏按画面、榜单、聊天排列。只将本页新收到且去重后的实时广播用于屏幕飘字，历史查询不触发飘字。画面输入列表最多 24 条，最多同时显示五条，八秒后清理；超出并发装饰容量直接省略飘字，正式聊天仍按自身容量展示。关闭弹幕、关房或卸载时清理动画计时器；减少动态效果偏好下不播放飘字。

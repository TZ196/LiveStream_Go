# LiveStream WebSocket 协议

基线 V1.1，协议 version=1。当前已实现共享票据/连接、点赞与弹幕独立能力、弹幕错误帧及历史补查；完整在线人数、连接标识和自定义关闭码仍未交付。此契约负责握手、消息、连接和前端行为。请求幂等算法见 [可靠性设计](reliability.md)，握手票据签发见 [HTTP 接口](api-spec.md)。

## 握手及连接生命周期

1. 前端带 Bearer 请求房间 ws-ticket，取得 30 秒内有效的一次性票据。
2. 连接 /ws/rooms/{room_id}?ticket=...；服务端先检查路径、Origin 白名单、票据摘要、绑定的用户及房间、票据和 JWT 到期时间。
3. 原子消费票据，检查开放场次后升级。点赞恢复状态不阻止共享握手；升级失败不恢复已消费票据。
4. 注册前在准入锁下再次检查同一开放场次和 API 停止状态，不符合则关闭连接。注册后先由唯一写循环发送 ROOM_STATE，期间增量进入有界发送队列。点赞首屏不可用时计数、epoch 和 revision 为 null；弹幕能力独立。
5. 主动退出、心跳超时、令牌过期或服务端关闭都走同一清理路径；注销成功一次才减一次连接数。

票据 HTTP 签发按 API 错误外壳返回。当前握手无效/过期/已用/Origin 不符统一为 HTTP 401 文本 invalid ticket，关闭或场次变化返回 409；不伪称已实现更细的握手错误码或全局连接配额。鉴权失败不得先升级再发送业务数据。票据绑定的 Origin 必须与握手一致。

票据和已连接状态绑定用户、房间、场次及认证到期时间。每次业务请求检查当前时间，到期关闭；当前不保证先发送 AUTH_EXPIRED 或自定义 4001 关闭帧。connection_id 仍为后续协议字段。客户端重新登录、申请新票据后重连，不能仅靠保活延长身份有效期。

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
| ROOM_STATE | 无 | room_id、session_id、status、like_count、likes_available、likes_state、projection_epoch、projection_generation、likes_revision、danmaku_available、delivery_modes、history_available、delivery_probe |
| DANMAKU_BROADCAST | event_id、request_id | room_id、session_id、user_id、username、content、created_at |
| LIKE_UPDATE | 无 | room_id、session_id、like_count、projection_epoch、likes_revision |
| LIKE_BOARD_DELTA | 无 | room_id、session_id、projection_epoch、base_revision、revision、size、changes |
| LIKE_BOARD_REFRESH | 无 | room_id、session_id、projection_epoch、revision |
| MY_LIKES_UPDATE | 无 | session_id、projection_epoch、likes_revision、count，只送本人在本场的连接 |
| ROOM_ONLINE_UPDATE（后续） | 无 | room_id、online_count、server_instance_id、presence_revision |
| ROOM_CLOSED（通知后续补齐） | 无 | room_id、closed_at、reason=owner_closed；当前关房关闭 socket，前端重新查询确认 |
| PONG | 无 | nonce、server_time_ms |
| ACK、ERROR | 按前文 | 按前文 |

ROOM_STATE 每个业务单独报告能力。delivery_modes.LIKE/DANMAKU 在启用时为 "kafka"，未启用为 null；弹幕暂不可用时 仍保留已启用的模式。history_available 表示弹幕历史路由存在，词库错误不关闭历史。status 为 OPEN。上述字段是当前实际首屏；HTTP RoomStats 的在线数/进程标识不在本轮交付中。

点赞状态变化或弹幕能力变化后，共享连接收到新的 ROOM_STATE。projection_generation 为本 API 进程内递增的十进制字符串；前端在同一物理连接拒绝较小 generation，在新连接重新建立比较基线。epoch 改变后清空旧点赞/榜单基线，LIKE_UPDATE 和榜单仅采用当前 epoch。点赞恢复不关闭共享连接；LIKES_UNAVAILABLE 只暂停点赞采集后的发送，不暂停弹幕。

弹幕广播按 event_id 去重。点赞 ACK 只确认发送者对应的批次。LIKE_UPDATE 是全场绝对总数的快照，可以覆盖多个用户的多个批次。快照和 ACK 可以先后独立到达，只有 ACK 能移除待确认批次。

likes_revision 为同一 projection_epoch 及 session_id 内的单调递增字符串整数，每个首次处理的批次按 count 增加，重复事件保持原值。API 每 200 毫秒向有观众且有变化的场次推送一次最新快照，空闲周期没有消息。客户端在同一场次、同一投影代次只采用较新或相同的版本。presence_revision 从进程级递增序列分配，每个房间保留最近值，回收空房间再建立时也不回退；server_instance_id 在进程启动时重新生成。旧版本状态不得覆盖新版本。发现投影 epoch 改变时采用新的能力快照并重新取榜单，过滤旧 epoch 的迟到帧。presence_revision/server_instance_id 是未交付的在线协议目标。

每次加入或重连，前端收到带场次和投影代次的 ROOM_STATE 后，从 HTTP 获取本场榜单和本人累计数。API 每秒合并一次公共榜单变化。LIKE_BOARD_DELTA 中的 changes 为发生变化的完整名次行，字段与 HTTP top 一致。客户端从 base_revision 对应的榜单替换这些名次，将长度设为 size，再采用 revision。基准版本缺失、补丁无效或收到 LIKE_BOARD_REFRESH 时，重新获取一次 HTTP 快照。差量帧编码后超过 8192 字节时服务端只发刷新通知。

MY_LIKES_UPDATE 带最终累计数，包括榜外用户的点击。一个用户打开多个标签页时，这些连接同时收到本人更新，其他用户只接收公开榜单和房间总数。个人数按 likes_revision 比较，公共榜单按 revision 比较。两者独立过滤迟到数据，换场或换代后重新取得快照。

## 心跳及退出

服务端每 20 秒发送 WebSocket 控制 Ping，浏览器协议栈响应控制 Pong，60 秒读期限用于检测断线。每条连接的发送队列容量为 128。遇满立即封闭本连接的入队入口，由已有连接协程关闭 socket。正在执行的写入有 5 秒期限。

### 点赞总数送达采样

API 从第一个连接起每 64 条新连接选一条，在首屏 `ROOM_STATE.data.delivery_probe` 标明。只有识别此能力的客户端才启用校时。控制消息使用 version=1、当前 session_id，无业务 request_id。

1. 浏览器立即发送应用层 `PING`，之后每 30 秒再发一次。data.nonce 为 1 至 64 字节的字符串。服务端 `PONG.data` 返回相同 nonce 和 `server_time_ms`（处理 PING 时的 Unix 毫秒）。浏览器以发出和收到之间的单调时间中点对齐服务器时间。60 秒后校时过期，连接切换时清除。
2. 对已启用采样的连接，写协程最多每 30 秒在一条 `LIKE_UPDATE` 顶层加入 `server_sent_at_ms`，取自即将编码并写出该帧时的服务端时钟。浏览器在 message 回调入口记下单调时间，用独立的校时结果换算成服务端时间。
3. 浏览器尽力回传 `DELIVERY_RECEIPT.data`，原样带回 `server_sent_at_ms`。有效校时还携带 `received_at_ms` 和 `uncertainty_ms`。误差估计包括校时半往返、1 ms 量化余量及按 200 ppm 估计的漂移。主线程等待也计入接收延迟。回执不等待 Vue 更新或屏幕绘制。
4. 每条连接只保留一个待回执样本。服务端只接纳本连接匹配且 15 秒内的首次回执。缺少校时或误差估计超过 50 ms 时只统计服务端单调时钟往返。超出往返可解释区间的值单独计数。误差内的负单程估计按零记录。查看单程分位数时须同时核对误差分布、排除数及未回执数。旧客户端仍可接收常规帧。控制消息不进入 Kafka，不影响业务 ACK 或重试。

以下自定义关闭码为后续目标，当前物理 Close 不保证发送这些码；客户端还必须查询房间状态。

| 关闭码 | 含义 | 是否自动重连 |
| --- | --- | --- |
| 1000 | 用户离开 | 否 |
| 1008 | 协议或来源策略拒绝 | 否 |
| 1009 | 消息过大 | 否 |
| 1012 | 服务重启（目标码） | 可以，重新申请票据 |
| 1013 | 过载或慢客户端 | 可以，退避 |
| 4001 | 认证到期 | 先重新登录 |
| 4004 | 房主关闭房间 | 否 |

错误或关闭通知尽力发送，不能保证断网客户端收到。按关闭码和重新查询的房间状态决定后续行为。

## 前端责任

HTTP 与 WebSocket 分别集中封装，页面只使用明确的业务状态。断线重连基准为 1、2、4、8、16、30 秒，乘以 0.8–1.2 的随机系数，实际等待最多 30 秒。收到首个有效 ROOM_STATE 后连接稳定三十秒才重置退避，后续能力快照不重置它，避免慢连接刚握手又断开时始终快速重试。同一页面只建立一条房间连接，两项业务分别订阅和发送。主动离开、房间关闭、认证失效或组件卸载时取消重试和稳定计时器、关闭连接；离开页面时由业务订阅方清空监听器。重连不恢复旧连接的发送队列，已落库弹幕通过历史补查，未确认发送沿用原请求 ID。

同一标签页内按用户、房间和场次聚合点赞。HTTP likes-state 在 Redis 恢复期间也返回当前场次；已确认开放场次后，断线、CHECKING、RECOVERING、BLOCKED 期间继续收集点击，内部保留累计数与待确认记录，页面只给简短连接或恢复提示。服务端权威总数显示在主播头像旁，页面不展示本地待发送、待确认数量。

在线第一下点击开启固定 500 毫秒窗口，到时发送批次。未发送点击以十进制累计数保存，首次发送前才生成 UUIDv7 并冻结数量，保存后调用 send。已尝试发送的批次不再并入新点击，重试沿用 ID、场次、数量。每次连接一轮最多 5 次、ACK 超时 3 秒；建立新物理连接并取得同场次有效 ROOM_STATE 后开启新一轮；同连接能力刷新不会重置重试次数，超过原请求 24 小时则保留记录和未确认提示，不更换 ID 补发。收到 accepted ACK 后清理对应批次，ACK 不代表数据库落盘。

最多保留 100 个冻结待确认批次，窗口满时继续累积未发送计数；计数使用 BigInt 与十进制字符串保存，发出时按 JavaScript 安全整数范围拆分。每 100 毫秒最多发送 10 个批次，并检查 WebSocket bufferedAmount，不将这些保护阈值当作实测容量。

sessionStorage 按用户、房间、场次保存累计数与冻结批次，同标签页刷新可恢复，兼容原数组记录。新场次不加载旧场次缓存。关闭房间、账号改变或原请求过期时不把旧记录转发为新操作。浏览器无法存储时提示本页记录无法持久保存。

共享连接建立前以 likes-state 取得 MySQL 场次，忽略点赞状态门槛；连接保持期间由 ROOM_STATE 能力更新驱动恢复，不再为点赞单独反复关闭连接。网络或连接失败按上述退避；等待恢复不消耗发送次数。只有状态 READY、WebSocket 已开并收到同场次有效 ROOM_STATE 才开始补发，打开 socket 本身不足以允许发送。离开页面取消探测、发送和连接超时计时器，释放连接及监听器。

弹幕前端内部维护最多 20 条待确认记录和 5000 个 event_id 去重，聊天列表最多 200 条。待确认队列按顺序发送，前一条未确认时不自动越过；3 秒 ACK 超时、最多 5 次自动尝试，耗尽后保留原 ID，通过一条简短状态和“重试确认”入口核对；页面不展示队列明细或数量。明确且不可重试的拒绝移出本地队列，CONTENT_BLOCKED 只显示 3.5 秒的通用审核提示；仅当输入框为空且对应最后一次提交时恢复原文草稿，不能覆盖后来输入或提交的内容。修改后用户主动发送生成新 ID，拒绝内容不会自动重发或进入聊天。旧缓存中的 rejected 记录清理，pending/unconfirmed 继续保留原身份。sessionStorage 按用户、房间、场次隔离；旧场缓存不送入新场。输入字符数按 Unicode 码点计算，Enter 发送，Shift+Enter 换行，输入法组合期间不触发发送。

浏览器重连取得 ROOM_STATE 后重新补查历史，即使列表已有消息；刷新历史重新建立分页游标。历史中与用户、场次、request_id、原文均匹配的持久化记录可证明原弹幕已接受，解除未确认。实时与历史按 event_id 合并，纯文本展示，不使用 v-html。仅补查已落库记录且有分页与列表容量边界，不承诺离线即时消息全部必达。

Redis 请求身份变化后的弹幕保护期、冻结版本和未知结果规则见可靠性设计；前端保留草稿和原请求，不自动改 ID 规避保护。

直播详情采用左侧 16:9 静态演示画面、右侧上方可收起的点赞榜、右侧下方弹幕聊天和固定输入框；窄屏按画面、榜单、聊天排列。只将本页新收到且去重后的实时广播用于屏幕飘字，历史查询不触发飘字。画面输入列表最多 24 条，最多同时显示五条，八秒后清理；超出并发装饰容量直接省略飘字，正式聊天仍按自身容量展示。关闭弹幕、关房或卸载时清理动画计时器；减少动态效果偏好下不播放飘字。

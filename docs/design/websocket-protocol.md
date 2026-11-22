# LiveStream WebSocket 协议

基线 V1.1，协议 version=1。当前已实现票据和点赞子集，ROOM_STATE 当前仅返回房间、场次和点赞投影字段；弹幕、在线人数、完整错误帧、自动历史恢复及全部连接状态字段尚未实现。此契约负责握手、消息、连接和前端行为。请求幂等算法见 [可靠性设计](reliability.md)，握手票据签发见 [HTTP 接口](api-spec.md)。

## 握手及连接生命周期

1. 前端带 Bearer 请求房间 ws-ticket，取得 30 秒内有效的一次性票据。
2. 连接 /ws/rooms/{room_id}?ticket=...；服务端先检查路径、Origin 白名单、票据摘要、绑定的用户及房间、票据和 JWT 到期时间。
3. 在房间准入控制下再次检查开放状态，预留连接容量并原子消费票据；释放准入锁后升级。升级失败释放预留容量，不恢复票据。
4. 注册 RoomHub 前再次检查房间状态；若升级期间房间已关闭，释放预留并以 4004 关闭，不计数。注册成功才加一；先有界缓冲增量，将 ROOM_STATE 快照置于发送队列首项，再按版本合并缓冲消息，避免快照读取期间漏掉状态变化。
5. 主动退出、心跳超时、令牌过期或服务端关闭都走同一清理路径；注销成功一次才减一次连接数。

缺失或无效票据返回 HTTP 401 TICKET_INVALID，过期返回 401 TICKET_EXPIRED，已消费返回 401 TICKET_USED；Origin 不匹配为 403，关闭房间为 409，容量不足为 429。鉴权失败不得先升级再发送业务数据。票据绑定的 Origin 必须与握手一致。

P3 场次模型启用后，票据和已连接状态绑定 user_id、room_id、session_id、connection_id 和 auth_expires_at；P3 的场次协议改动随实现交付。每次业务请求检查当前时间；到期主动发送 AUTH_EXPIRED 并使用关闭码 4001 断开。客户端重新登录、申请新票据后重连，不能仅靠保活延长身份有效期。

## 帧约定

使用 UTF-8 文本 JSON 帧，拒绝非法 UTF-8，不接收二进制业务帧。version 必须为 1；客户端消息只接受定义的字段。单消息最多 8 KiB，超出使用关闭码 1009。弹幕不接受 NUL 和除制表符、换行外的控制字符；文本只作为文本渲染。

业务 request_id 为规范小写 UUIDv7，由客户端在消息产生时生成并保留；事件时间从该 ID 提取，客户端时钟需满足校验窗口。同一次动作重试必须保留 request_id、session_id、消息类型和 data，不合并后续点击。DANMAKU.data 只有 content；LIKE.data 为 {}，固定 +1，不接收 count。room_id 来自连接；客户端传入原始 session_id 以保证重试不意外改绑，服务端验证其来源与授权，不能仅因客户端提供就信任。场次结束后的旧请求不能改绑新场；没有请求登记时无法判断旧请求是否曾准入，安全处理仍待设计。

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
  "data": {}
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

ACK 只表示当前阶段的接受边界已满足；P3 弹幕为 memory，P4 点赞和 P5 弹幕为 kafka，见开发计划。重复请求返回相同 event_id；没有请求登记时无法保证首次接受时间一致，accepted_at 可以为 null。

Kafka 结果不确定时不向前端发送包含 Kafka 确认丢失细节或 RESULT_UNKNOWN 的消息，也不伪造成功或明确失败；服务端在预算内使用相同事件有界重试，前端未收到 ACK 时保留原请求并按有限重试规则处理，不把后续点击并入旧请求。重试耗尽仍无结果时停止自动重发；后台保留诊断。长期未确认的安全查询或处理机制需在上线前确定，不得假定消息未发生。

仅有确切拒绝依据时返回 ERROR；顶层 request_id、event_id 无法确定时为 null；data 包含 code、message、outcome、retryable、retry_after_ms。不能因网络中断或记录不可读就断言操作未发生。

| code | outcome | 客户端行为 |
| --- | --- | --- |
| INVALID_ARGUMENT、UNSUPPORTED_MESSAGE | rejected | 不自动重试；修改内容属于新的用户动作 |
| CLOCK_SKEW | rejected | 校正客户端时间，禁止自动修改未知操作的 ID |
| REQUEST_EXPIRED | rejected | 仅能证明从未接受时返回；无法确定历史结果时后台保留未确认并停止自动重发，不对前端发送未知结果 |
| ROOM_CLOSED、ROOM_NOT_FOUND、AUTH_EXPIRED | rejected | 离开房间或重新认证 |
| RATE_LIMITED、SERVER_BUSY、DEPENDENCY_UNAVAILABLE、RECOVERING | rejected | 按 retry_after_ms 用原请求重试；此响应仅在尚未开始发布时使用 |
| REQUEST_IN_PROGRESS | pending | 原请求等待 1 秒后重试 |
| CAPABILITY_NOT_ENABLED | rejected | 当前阶段隐藏未交付操作 |

outcome 描述当前业务动作的已知结果，不只描述本次网络尝试。不能因本次未执行就宣称原操作从未发生；不得将 Kafka 内部的 UNKNOWN 原样返回前端，安全的长期处理方式待设计。相同 ID 的不同内容无法在发布前直接判定冲突。

## 服务端消息

所有消息含 version、type、data。下表给出此外的字段和 data 的完整业务字段；HTTP 公共字段类型约定同样适用。

| type | 顶层附加字段 | data |
| --- | --- | --- |
| ROOM_STATE | 无 | room_id、session_id、status、connection_id、auth_expires_at、online_count、server_instance_id、presence_revision、like_count、likes_available、projection_epoch、likes_revision、delivery_modes、history_available |
| DANMAKU_BROADCAST | event_id、request_id | room_id、session_id、user_id、username、content、created_at |
| LIKE_UPDATE | event_id、request_id | room_id、session_id、like_count、projection_epoch、likes_revision |
| ROOM_ONLINE_UPDATE | 无 | room_id、online_count、server_instance_id、presence_revision |
| ROOM_CLOSED | 无 | room_id、closed_at、reason=owner_closed |
| PONG | 无 | nonce、server_time |
| ACK、ERROR | 按前文 | 按前文 |

ROOM_STATE 的统计字段与 HTTP RoomStats 一致；delivery_modes 为按消息类型区分的对象，例如 P3 `{"DANMAKU":"memory","LIKE":null}`，P4 `{"DANMAKU":"memory","LIKE":"kafka"}`，P5 起 `{"DANMAKU":"kafka","LIKE":"kafka"}`。单一 delivery_mode 无法表达 P4 两条不同链路，协议未实现，交付前须同步前端和验收。history_available 指弹幕历史，在 P6 完成后为 true；P4 点赞写库不意味着弹幕历史已交付。连接和进程标识为 UUID 字符串，status 使用 OPEN/CLOSED。

业务事件通过 event_id 去重；request_id 只与当前用户自己的待确认操作关联，不能单凭其他用户的 request_id 合并。广播可能先于 ACK，收到广播后展示消息，后到 ACK 只更新发送状态。

likes_revision 为同一 projection_epoch 及 session_id 内的单调递增字符串整数，每个首次有效点赞加一；重复事件不增加。presence_revision 从进程级递增序列分配，每个房间保留最近值，回收空房间再建立时也不回退；server_instance_id 在进程启动时重新生成。旧版本状态不得覆盖新版本。发现投影 epoch 改变时重新取快照；恢复流程会关闭旧连接以隔离旧状态。

## 心跳及退出

服务端每 20 秒发送 WebSocket 控制 Ping，浏览器协议栈响应控制 Pong；60 秒未收到有效响应则关闭。应用层可选发送 {"version":1,"type":"PING","data":{"nonce":"..."}}，服务端 PONG 原样返回 nonce 并加 server_time；nonce 最多 64 字节。应用 PING 不参与业务幂等和 Kafka。

| 关闭码 | 含义 | 是否自动重连 |
| --- | --- | --- |
| 1000 | 用户离开 | 否 |
| 1008 | 协议或来源策略拒绝 | 否 |
| 1009 | 消息过大 | 否 |
| 1012 | 服务重启或投影恢复 | 可以，重新申请票据 |
| 1013 | 过载或慢客户端 | 可以，退避 |
| 4001 | 认证到期 | 先重新登录 |
| 4004 | 房主关闭房间 | 否 |

错误或关闭通知尽力发送，不能保证断网客户端收到。按关闭码和重新查询的房间状态决定后续行为。

## 前端责任

HTTP 与 WebSocket 分别集中封装，页面只使用明确的业务状态。断线重连使用 1、2、4、8、16、30 秒上限的指数退避并加抖动；每轮最多 10 次，之后提供手动重连。主动离开或组件卸载取消定时器、关闭连接、清空监听器。

待确认业务请求每轮最多自动重试 5 次；不向用户展示 Kafka 内部确认丢失信息，仍未确认时停止自动重发并保留原请求；安全的后续查询能力待设计；在 24 小时窗口内保留原标识，不创建替代操作。每个标签页最多保存 100 条待确认请求到 sessionStorage，绑定 user_id，清除后不自动恢复发送；访问令牌不写入该存储。当前每次请求最多自动发送 5 次，超过后保留待确认状态。

前端维护有界事件去重缓存，最多 5000 个 event_id，消息列表默认最多 1000 条。浏览器重连后获取 ROOM_STATE，再加载可用历史，并与实时事件合并；不承诺恢复离线期间所有即时推送。

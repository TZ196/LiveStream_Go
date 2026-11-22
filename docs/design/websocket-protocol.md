# LiveStream WebSocket 协议

基线 V1.1，协议 version=1。此契约负责握手、消息、连接和前端行为。请求幂等算法见 [可靠性设计](reliability.md)，握手票据签发见 [HTTP 接口](api-spec.md)。

## 握手及连接生命周期

1. 前端带 Bearer 请求房间 ws-ticket，取得 30 秒内有效的一次性票据。
2. 连接 /ws/rooms/{room_id}?ticket=...；服务端先检查路径、Origin 白名单、票据摘要、绑定的用户及房间、票据和 JWT 到期时间。
3. 在房间准入控制下再次检查开放状态，预留连接容量并原子消费票据；释放准入锁后升级。升级失败释放预留容量，不恢复票据。
4. 注册 RoomHub 前再次检查房间状态；若升级期间房间已关闭，释放预留并以 4004 关闭，不计数。注册成功才加一；先有界缓冲增量，将 ROOM_STATE 快照置于发送队列首项，再按版本合并缓冲消息，避免快照读取期间漏掉状态变化。
5. 主动退出、心跳超时、令牌过期或服务端关闭都走同一清理路径；注销成功一次才减一次连接数。

缺失或无效票据返回 HTTP 401 TICKET_INVALID，过期返回 401 TICKET_EXPIRED，已消费返回 401 TICKET_USED；Origin 不匹配为 403，关闭房间为 409，容量不足为 429。鉴权失败不得先升级再发送业务数据。票据绑定的 Origin 必须与握手一致。

已连接状态保存 user_id、room_id、connection_id 和 auth_expires_at。每次业务请求检查当前时间；到期主动发送 AUTH_EXPIRED 并使用关闭码 4001 断开。客户端重新登录、申请新票据后重连，不能仅靠保活延长身份有效期。

## 帧约定

使用 UTF-8 文本 JSON 帧，拒绝非法 UTF-8，不接收二进制业务帧。version 必须为 1；客户端消息只接受定义的字段。单消息最多 8 KiB，超出使用关闭码 1009。弹幕不接受 NUL 和除制表符、换行外的控制字符；文本只作为文本渲染。

业务 request_id 为规范小写 UUIDv7。同一次动作重试必须保留 request_id、消息类型和 data。DANMAKU.data 只有 content；LIKE.data 为 {}。room_id 来自连接，不允许客户端改变连接绑定的房间。

```json
{
  "version": 1,
  "type": "DANMAKU",
  "request_id": "01a1edab-7800-7abc-8000-123456789abc",
  "data": {"content": "大家好"}
}
```

```json
{
  "version": 1,
  "type": "LIKE",
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
  "event_id": "62eac618-3791-5fc5-b572-a20981c36e6f",
  "data": {
    "status": "accepted",
    "accepted_via": "kafka",
    "accepted_at": "2026-11-18T08:00:00.010Z"
  }
}
```

ACK 只表示当前阶段的接受边界已满足，accepted_via 为 memory、redis 或 kafka，见开发计划。重复请求返回相同 event_id 和首次已知接受时间；恢复后无法取得原 ACK 时间时允许 accepted_at=null，但事件身份保持不变。

```json
{
  "version": 1,
  "type": "ERROR",
  "request_id": "01a1edab-7801-7abc-8000-123456789abc",
  "event_id": "5e808eea-b244-55cc-98f5-c0799dd6c66d",
  "data": {
    "code": "RESULT_UNKNOWN",
    "message": "操作结果待确认，请保留原请求标识重试",
    "outcome": "unknown",
    "retryable": true,
    "retry_after_ms": 1000
  }
}
```

ERROR 顶层 request_id、event_id 无法确定时为 null；data 必须包含 code、message、outcome、retryable、retry_after_ms。outcome 为 rejected、pending 或 unknown。pending/unknown 不能显示为“确定未发生”；重试只能使用原请求。

| code | outcome | 客户端行为 |
| --- | --- | --- |
| INVALID_ARGUMENT、UNSUPPORTED_MESSAGE、REQUEST_CONFLICT | rejected | 不自动重试；修改内容属于新的用户动作 |
| CLOCK_SKEW | rejected | 校正客户端时间，禁止自动修改未知操作的 ID |
| REQUEST_EXPIRED | rejected 或 unknown | 仅能证明从未接受时为 rejected；无法确定历史结果时保留 unknown，停止自动重发 |
| ROOM_CLOSED、ROOM_NOT_FOUND、AUTH_EXPIRED | rejected | 离开房间或重新认证 |
| RATE_LIMITED、SERVER_BUSY、DEPENDENCY_UNAVAILABLE、RECOVERING | rejected | 按 retry_after_ms 用原请求重试；此响应仅在尚未开始发布时使用 |
| REQUEST_IN_PROGRESS | pending | 原请求等待 1 秒后重试 |
| RESULT_UNKNOWN | unknown | 保留原 ID 与内容，退避重试或显示待确认 |
| CAPABILITY_NOT_ENABLED | rejected | 当前阶段隐藏未交付操作 |

outcome 描述当前业务动作的已知结果，不只描述本次网络尝试。请求记录不可读、已丢失或已过期时，不能因本次未执行就宣称原操作从未发生；此时使用 RESULT_UNKNOWN，或不可再发布时使用 REQUEST_EXPIRED 加 unknown。REQUEST_CONFLICT 拒绝的是不同内容的请求，不撤销原操作。

## 服务端消息

所有消息含 version、type、data。下表给出此外的字段和 data 的完整业务字段；HTTP 公共字段类型约定同样适用。

| type | 顶层附加字段 | data |
| --- | --- | --- |
| ROOM_STATE | 无 | room_id、status、connection_id、auth_expires_at、online_count、server_instance_id、presence_revision、like_count、likes_available、projection_epoch、likes_revision、delivery_mode、history_available |
| DANMAKU_BROADCAST | event_id、request_id | room_id、user_id、username、content、created_at |
| LIKE_UPDATE | event_id、request_id | room_id、like_count、projection_epoch、likes_revision |
| ROOM_ONLINE_UPDATE | 无 | room_id、online_count、server_instance_id、presence_revision |
| ROOM_CLOSED | 无 | room_id、closed_at、reason=owner_closed |
| PONG | 无 | nonce、server_time |
| ACK、ERROR | 按前文 | 按前文 |

ROOM_STATE 的统计字段与 HTTP RoomStats 一致；delivery_mode 为 direct 或 kafka，history_available 在 P6 完成后为 true。连接和进程标识为 UUID 字符串，status 使用 OPEN/CLOSED。

业务事件通过 event_id 去重；request_id 只与当前用户自己的待确认操作关联，不能单凭其他用户的 request_id 合并。广播可能先于 ACK，收到广播后展示消息，后到 ACK 只更新发送状态。

likes_revision 为同一 projection_epoch 内的单调递增字符串整数，每个首次有效点赞加一；重复事件不增加。presence_revision 从进程级递增序列分配，每个房间保留最近值，回收空房间再建立时也不回退；server_instance_id 在进程启动时重新生成。旧版本状态不得覆盖新版本。发现投影 epoch 改变时重新取快照；恢复流程会关闭旧连接以隔离旧状态。

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

待确认业务请求每轮最多自动重试 5 次，仍未知则由用户选择继续查询；在 24 小时窗口内保留原标识，不创建替代操作。每个标签页最多保存 100 条待确认请求到 sessionStorage，绑定 user_id，清除后不自动恢复发送；访问令牌不写入该存储。

前端维护有界事件去重缓存，最多 5000 个 event_id，消息列表默认最多 1000 条。浏览器重连后获取 ROOM_STATE，再加载可用历史，并与实时事件合并；不承诺恢复离线期间所有即时推送。
